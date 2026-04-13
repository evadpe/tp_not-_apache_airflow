# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie
## Auteur
- Nom : DEPAEPE 
- Prénom : Eva
- Formation : Dev Big Data IA Ipssi
- Date : 13/04/2026
## Prérequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- Python >= 3.11 (pour les tests locaux)

## Instructions de déploiement

### 1. Démarrage de la stack

Cloner le dépôt 
git clone https://github.com/evadpe/tp_not-_apache_airflow.git


# Initialiser la base de métadonnées Airflow et créer l'utilisateur admin
docker compose run --rm airflow-init

# Démarrer tous les services 
docker compose up -d

# Vérifier que les 7 services sont bien démarrés
docker compose ps


Les services démarrés sont :
- `postgres` — base de métadonnées Airflow
- `postgres-ars` — base de données ARS Occitanie
- `redis` — broker CeleryExecutor
- `airflow-webserver` — interface web (port 8080)
- `airflow-scheduler` — ordonnancement des DAGs
- `airflow-worker` — exécution des tâches (Celery)
- `flower` — monitoring des workers Celery (port 5555)

### 2. Configuration des connexions et variables Airflow

Dans l'interface Airflow (http://localhost:8080), créer les connexions comme dans l'énoncé du tp: 

Connection Id : postgres_ars
Connection Type : Postgres
Host : postgres-ars
Database : ars_epidemio
Login : ars_user
Password : ars_password
Port : 5432

Pareil pour les variables (comme dans le tp également)

### 3. Démarrage du pipeline

Aller sur http://localhost:8080 et activer le DAG ars_epidemio_dag

## Architecture des données

Les fichiers JSON sont organisés selon la structure suivante dans le volume Docker /data/ars :

/data/ars/
raw/<année>/ S<semaine>/sursaud_<semaine>.json
indicateurs/indicateurs_<semaine>.json
rapports/<année>/ S<semaine>/rapport_<semaine>.json

Il y a 4 tables dans postgreSQL : 
- syndromes : GRIPPE, GEA, SG, BRONCHIO, COVID19
- departements : 13 départements d'Occitanie
- donnees_hebdomadaires — valeurs IAS agrégées par semaine et syndrome
- indicateurs_epidemiques — z-score, R0 estimé et statut
- rapports_ars : — rapports hebdomadaires

## Décisions techniques

- CeleryExecutor : j'ai choisi Celery plutôt que LocalExecutor parce que ça permet d'avoir un vrai worker séparé, ce qui est plus proche d'une infra réelle. 
- Image apache/airflow:2.8.0 : j'ai gardé une version fixe plutôt que latest pour éviter les mauvaises surprises si l'image est mise à jour entre deux runs.
- Deux bases PostgreSQL séparées : une pour les métadonnées Airflow, une pour les données ARS. Comme ça si on veut sauvegarder ou réinitialiser l'une sans toucher à l'autre, c'est possible.
- Volumes nommés Docker : pour que les données survivent à un docker compose down. Sinon on perd tout à chaque redémarrage.
- ON CONFLICT DO UPDATE partout dans les INSERT : si on relance le DAG sur la même semaine, ça écrase proprement sans créer de doublons. (pour l'indempotence demandée)
- from __future__ import annotations : obligatoire pour utiliser les type hints style Python 3.10+ (`list[float]` etc.) sur Python 3.8 qui est la version dans les containers Airflow 2.8.

## Difficultés rencontrées et solutions

- Erreur NotNullViolation sur valeur_ias : la colonne est définie NOT NULL dans la table mais quand il n'y a pas de données CSV pour une semaine donnée, la valeur remontée est None. Ça faisait planter le pipeline. Réglé en ajoutant un if data.get("valeur_ias") is not None avant chaque INSERT pour ignorer les lignes vides.
- Trouver la bonne colonne Occitanie dans les CSV : les fichiers IAS sur data.gouv.fr utilisent encore les anciens codes régionaux d'avant 2016, donc Occitanie n'existe pas il faut utiliser Loc_Reg76. Mais ça vous l'avez corrigé dans l'enoncé tu tp.
- generer_rapport_hebdomadaire restait en skipped : avec le BranchPythonOperator, les branches non empruntées sont automatiquement skippées par Airflow, ce qui faisait passer generer_rapport en skipped aussi. J'ai ajouté trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS sur cette tâche et ça a fonctionné.
