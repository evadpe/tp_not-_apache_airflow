from datetime import datetime, timedelta
import sys
import os
import shutil
import logging
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

logger = logging.getLogger(__name__)

# --- FONCTIONS DU DAG ---

def collecter_donnees_ias(**context) -> str:
    """Télécharge les CSV IAS® et retourne le chemin du fichier JSON créé."""
    execution_date = context['execution_date']
    year, week, _ = execution_date.isocalendar()
    semaine = f"{year}-S{week:02d}"
    
    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = f"{archive_path}/raw"
    
    sys.path.insert(0, "/opt/airflow/scripts")
    from collecte_ias import (
        DATASETS_IAS, telecharger_csv_ias, filtrer_semaine,
        agreger_semaine, sauvegarder_donnees
    )
    
    resultats = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)
    
    return sauvegarder_donnees(resultats, semaine, output_dir)

def archiver_local(**context) -> str:
    """Copie le fichier JSON dans l'arborescence d'archive structurée."""
    ti = context['ti']
    chemin_source = ti.xcom_pull(task_ids="collecter_donnees_sursaud")
    
    execution_date = context['execution_date']
    year, week, _ = execution_date.isocalendar()
    semaine = f"{year}-S{week:02d}"
    
    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    dest_dir = os.path.join(archive_path, "raw", str(year), f"S{week:02d}")
    os.makedirs(dest_dir, exist_ok=True)
    
    chemin_dest = os.path.join(dest_dir, f"sursaud_{semaine}.json")
    shutil.copy2(chemin_source, chemin_dest)
    
    logger.info(f"Archivé : {chemin_dest}")
    return chemin_dest

def calculer_indicateurs_epidemiques(**context) -> str:
    """Calcule les indicateurs épidémiques à partir du JSON archivé."""
    ti = context["ti"]
    chemin_json = ti.xcom_pull(task_ids="archiver_local")

    sys.path.insert(0, "/opt/airflow/scripts")
    from calcul_indicateurs import (
        charger_donnees_ias, calculer_indicateurs, sauvegarder_indicateurs
    )

    donnees = charger_donnees_ias(chemin_json)
    indicateurs = calculer_indicateurs(donnees)

    archive_path = Variable.get("archive_base_path", default_var="/data/ars")
    output_dir = os.path.join(archive_path, "indicateurs")

    execution_date = context["execution_date"]
    year, week, _ = execution_date.isocalendar()
    semaine = f"{year}-S{week:02d}"

    return sauvegarder_indicateurs(indicateurs, semaine, output_dir)

def verifier_archive(**context) -> None:
    """Vérifie que le fichier archivé existe et n'est pas vide."""
    ti = context['ti']
    chemin = ti.xcom_pull(task_ids="archiver_local")

    if not os.path.exists(chemin):
        raise FileNotFoundError(f"Fichier introuvable : {chemin}")
    if os.path.getsize(chemin) == 0:
        raise ValueError(f"Fichier vide : {chemin}")

    logger.info(f"Archive vérifiée OK : {chemin}")


def evaluer_situation_epidemique(**context) -> str:
    """Lit les indicateurs depuis PostgreSQL et retourne le task_id de la branche à exécuter."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    execution_date = context["execution_date"]
    year, week, _ = execution_date.isocalendar()
    semaine = f"{year}-S{week:02d}"

    hook = PostgresHook(postgres_conn_id="postgres_ars")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT statut, COUNT(*) AS nb, ARRAY_AGG(syndrome) AS syndromes
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                GROUP BY statut
            """, (semaine,))
            resultats = {row[0]: {"nb": row[1], "syndromes": row[2]} for row in cur.fetchall()}

    nb_urgence = resultats.get("URGENCE", {}).get("nb", 0)
    nb_alerte  = resultats.get("ALERTE",  {}).get("nb", 0)

    context["ti"].xcom_push(key="nb_urgence",        value=nb_urgence)
    context["ti"].xcom_push(key="nb_alerte",         value=nb_alerte)
    context["ti"].xcom_push(key="syndromes_urgence", value=resultats.get("URGENCE", {}).get("syndromes", []))
    context["ti"].xcom_push(key="syndromes_alerte",  value=resultats.get("ALERTE",  {}).get("syndromes", []))

    if nb_urgence > 0:
        return "declencher_alerte_ars"
    elif nb_alerte > 0:
        return "envoyer_bulletin_surveillance"
    else:
        return "confirmer_situation_normale"


def declencher_alerte_ars(**context) -> None:
    """Déclenche une alerte ARS pour les syndromes en URGENCE."""
    syndromes = context["ti"].xcom_pull(task_ids="evaluer_situation_epidemique", key="syndromes_urgence")
    logger.critical(f"ALERTE ARS DÉCLENCHÉE — Syndromes en URGENCE : {syndromes}")


def envoyer_bulletin_surveillance(**context) -> None:
    """Envoie un bulletin de surveillance pour les syndromes en ALERTE."""
    syndromes = context["ti"].xcom_pull(task_ids="evaluer_situation_epidemique", key="syndromes_alerte")
    logger.warning(f"Bulletin de surveillance — Syndromes en ALERTE : {syndromes}")


def confirmer_situation_normale(**_) -> None:
    """Confirme une situation épidémiologique normale."""
    logger.info("Situation épidémiologique normale en Occitanie — aucune action requise")


def generer_rapport_hebdomadaire(**context) -> None:
    """Génère le rapport hebdomadaire JSON et l'insère dans PostgreSQL."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from datetime import datetime as dt

    execution_date = context["execution_date"]
    year, week, _ = execution_date.isocalendar()
    semaine = f"{year}-S{week:02d}"
    annee = str(year)
    num_sem = f"S{week:02d}"

    hook = PostgresHook(postgres_conn_id="postgres_ars")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT syndrome, valeur_ias, z_score, r0_estime, statut,
                       statut_ias, statut_zscore, nb_saisons_reference
                FROM indicateurs_epidemiques
                WHERE semaine = %s
                ORDER BY statut DESC
            """, (semaine,))
            rows = cur.fetchall()

    statuts = [row[4] for row in rows]
    if "URGENCE" in statuts:
        situation_globale = "URGENCE"
    elif "ALERTE" in statuts:
        situation_globale = "ALERTE"
    else:
        situation_globale = "NORMAL"

    syndromes_urgence = [row[0] for row in rows if row[4] == "URGENCE"]
    syndromes_alerte  = [row[0] for row in rows if row[4] == "ALERTE"]

    recommandations = {
        "URGENCE": [
            "Activation du plan de réponse épidémique régional",
            "Notification immédiate à Santé Publique France et au Ministère de la Santé",
        ],
        "ALERTE": [
            "Surveillance renforcée des indicateurs pour les 48h suivantes",
            "Envoi d'un bulletin de surveillance aux partenaires de santé",
        ],
        "NORMAL": [
            "Maintien de la surveillance standard",
            "Prochain point épidémiologique dans 7 jours",
        ],
    }

    rapport = {
        "semaine":                    semaine,
        "region":                     "Occitanie",
        "code_region":                "76",
        "date_generation":            dt.utcnow().isoformat(),
        "situation_globale":          situation_globale,
        "nb_departements_surveilles": 13,
        "syndromes_en_urgence":       syndromes_urgence,
        "syndromes_en_alerte":        syndromes_alerte,
        "indicateurs": [
            {
                "syndrome":      row[0],
                "valeur_ias":    row[1],
                "z_score":       row[2],
                "r0_estime":     row[3],
                "statut":        row[4],
                "statut_ias":    row[5],
                "statut_zscore": row[6],
            }
            for row in rows
        ],
        "recommandations":  recommandations[situation_globale],
        "genere_par":       "ars_epidemio_dag v1.0",
        "pipeline_version": "2.8",
    }

    local_path = f"/data/ars/rapports/{annee}/{num_sem}/rapport_{semaine}.json"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(rapport, f, ensure_ascii=False, indent=2)

    hook2 = PostgresHook(postgres_conn_id="postgres_ars")
    with hook2.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rapports_ars
                    (semaine, situation_globale, nb_depts_alerte, nb_depts_urgence,
                     rapport_json, chemin_local)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (semaine) DO UPDATE SET
                    situation_globale = EXCLUDED.situation_globale,
                    nb_depts_alerte   = EXCLUDED.nb_depts_alerte,
                    nb_depts_urgence  = EXCLUDED.nb_depts_urgence,
                    rapport_json      = EXCLUDED.rapport_json,
                    chemin_local      = EXCLUDED.chemin_local,
                    updated_at        = CURRENT_TIMESTAMP
            """, (
                semaine,
                situation_globale,
                len(syndromes_alerte),
                len(syndromes_urgence),
                json.dumps(rapport, ensure_ascii=False),
                local_path,
            ))
        conn.commit()
    logger.info(f"Rapport {semaine} généré — statut : {situation_globale}")


def inserer_donnees_postgres(**context) -> None:
    """Insère les données hebdomadaires et les indicateurs dans PostgreSQL."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    execution_date = context["execution_date"]
    year, week, _ = execution_date.isocalendar()
    semaine = f"{year}-S{week:02d}"
    annee = str(year)
    num_sem = f"S{week:02d}"

    with open(f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json") as f:
        donnees_brutes = json.load(f)

    with open(f"/data/ars/indicateurs/indicateurs_{semaine}.json") as f:
        indicateurs = json.load(f)

    hook = PostgresHook(postgres_conn_id="postgres_ars")

    sql_donnees = """
        INSERT INTO donnees_hebdomadaires
            (semaine, syndrome, valeur_ias, seuil_min_saison, seuil_max_saison, nb_jours_donnees)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
        ON CONFLICT (semaine, syndrome) DO UPDATE SET
            valeur_ias       = EXCLUDED.valeur_ias,
            seuil_min_saison = EXCLUDED.seuil_min_saison,
            seuil_max_saison = EXCLUDED.seuil_max_saison,
            nb_jours_donnees = EXCLUDED.nb_jours_donnees,
            updated_at       = CURRENT_TIMESTAMP;
    """

    sql_indicateurs = """
        INSERT INTO indicateurs_epidemiques
            (semaine, syndrome, valeur_ias, z_score, r0_estime, nb_saisons_reference,
             statut, statut_ias, statut_zscore)
        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s, %(z_score)s, %(r0_estime)s,
                %(nb_saisons_reference)s, %(statut)s, %(statut_ias)s, %(statut_zscore)s)
        ON CONFLICT (semaine, syndrome) DO UPDATE SET
            valeur_ias           = EXCLUDED.valeur_ias,
            z_score              = EXCLUDED.z_score,
            r0_estime            = EXCLUDED.r0_estime,
            nb_saisons_reference = EXCLUDED.nb_saisons_reference,
            statut               = EXCLUDED.statut,
            statut_ias           = EXCLUDED.statut_ias,
            statut_zscore        = EXCLUDED.statut_zscore,
            updated_at           = CURRENT_TIMESTAMP;
    """

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            for _, data in donnees_brutes.get("syndromes", {}).items():
                if data.get("valeur_ias") is not None:
                    cur.execute(sql_donnees, data)
            for _, data in indicateurs.items():
                if data.get("valeur_ias") is not None:
                    cur.execute(sql_indicateurs, data)
        conn.commit()
    logger.info(f"{len(indicateurs)} indicateurs insérés pour la semaine {semaine}")

# --- CONFIGURATION DU DAG ---

default_args = {
    "owner": "ars-occitanie",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ars_epidemio_dag",
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie",
    schedule_interval="0 6 * * 1",   # Tous les lundis à 6h UTC
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:

    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True,
    )

    collecter_sursaud = PythonOperator(
        task_id="collecter_donnees_sursaud",
        python_callable=collecter_donnees_ias,
        provide_context=True,
    )

    archiver = PythonOperator(
        task_id="archiver_local",
        python_callable=archiver_local,
        provide_context=True,
    )

    verifier = PythonOperator(
        task_id="verifier_archive",
        python_callable=verifier_archive,
        provide_context=True,
    )

    calculer = PythonOperator(
        task_id="calculer_indicateurs_epidemiques",
        python_callable=calculer_indicateurs_epidemiques,
        provide_context=True,
    )

    inserer_postgres = PythonOperator(
        task_id="inserer_donnees_postgres",
        python_callable=inserer_donnees_postgres,
        provide_context=True,
    )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique,
        provide_context=True,
    )

    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars,
        provide_context=True,
    )

    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance,
        provide_context=True,
    )

    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale,
        provide_context=True,
    )

    init_base_donnees >> collecter_sursaud >> archiver >> verifier >> calculer >> inserer_postgres >> evaluer
    evaluer >> [alerte_ars, bulletin, normale]

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        provide_context=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    [alerte_ars, bulletin, normale] >> generer_rapport