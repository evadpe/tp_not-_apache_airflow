"""
Calcul des indicateurs épidémiques IAS® — ARS Occitanie
"""
from __future__ import annotations
import json
import logging
import os
from typing import Optional

import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculer_zscore(
    valeur_actuelle: float, historique: list[float]
) -> Optional[float]:
    """
    Calcule le z-score de la valeur IAS par rapport aux saisons historiques.
    Requiert au minimum 3 valeurs historiques.
    """
    valeurs_valides = [v for v in historique if v is not None]
    if len(valeurs_valides) < 3:
        logger.warning(f"Historique insuffisant ({len(valeurs_valides)} saisons)")
        return None

    moyenne = np.mean(valeurs_valides)
    ecart_type = np.std(valeurs_valides, ddof=1)

    if ecart_type == 0:
        return 0.0

    return float((valeur_actuelle - moyenne) / ecart_type)


def classifier_statut_ias(
    valeur_ias: float,
    seuil_min: Optional[float],
    seuil_max: Optional[float],
) -> str:
    """Classifie selon les seuils MIN/MAX du dataset IAS."""
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(
    z_score: Optional[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> str:
    """Classifie selon le z-score par rapport à l'historique des saisons."""
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    """Retient le niveau le plus sévère entre les deux critères."""
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(
    series_hebdomadaire: list[float],
    duree_infectieuse: int = 5,
) -> Optional[float]:
    """Estimation du R0 par calcul du taux de croissance moyen sur les séries IAS."""
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]

    if len(series_valides) < 2:
        return None

    croissances = [
        (series_valides[i] - series_valides[i - 1]) / series_valides[i - 1]
        for i in range(1, len(series_valides))
    ]

    if not croissances:
        return None

    return max(0.0, float(1 + np.mean(croissances) * (duree_infectieuse / 7)))


def charger_donnees_ias(chemin_json: str) -> dict:
    """Charge le fichier JSON produit par collecte_ias."""
    logger.info(f"Chargement des données : {chemin_json}")
    with open(chemin_json, "r", encoding="utf-8") as f:
        return json.load(f)


def calculer_indicateurs(donnees: dict) -> dict:
    """Orchestre le calcul des indicateurs pour chaque syndrome."""
    semaine = donnees.get("semaine")
    syndromes = donnees.get("syndromes", {})
    resultats = {}

    for syndrome, data in syndromes.items():
        valeur_ias = data.get("valeur_ias")
        seuil_min  = data.get("seuil_min")
        seuil_max  = data.get("seuil_max")
        historique = data.get("historique", {})

        valeurs_hist = [v for v in historique.values() if v is not None]

        z_score = None
        r0      = None

        if valeur_ias is not None:
            z_score = calculer_zscore(valeur_ias, valeurs_hist)
            if seuil_min is not None and seuil_min > 0:
                r0 = round(valeur_ias / seuil_min, 3)

        statut_ias = classifier_statut_ias(valeur_ias or 0.0, seuil_min, seuil_max)
        statut_z   = classifier_statut_zscore(z_score)
        statut     = classifier_statut_final(statut_ias, statut_z)

        resultats[syndrome] = {
            "semaine":              semaine,
            "syndrome":             syndrome,
            "valeur_ias":           valeur_ias,
            "z_score":              round(z_score, 3) if z_score is not None else None,
            "r0_estime":            r0,
            "nb_saisons_reference": len(valeurs_hist),
            "statut":               statut,
            "statut_ias":           statut_ias,
            "statut_zscore":        statut_z,
        }
        logger.info(f"{syndrome} {semaine} → statut={statut}, z_score={z_score}")

    return resultats


def sauvegarder_indicateurs(indicateurs: dict, semaine: str, output_dir: str) -> str:
    """Sauvegarde les indicateurs calculés en JSON."""
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"indicateurs_{semaine}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(indicateurs, f, ensure_ascii=False, indent=2)
    logger.info(f"Indicateurs sauvegardés : {output_path}")
    return output_path
