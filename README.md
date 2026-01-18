# EDT CESI - Plateforme de Diffusion de Planning Dynamique

## Présentation
EDT CESI est une solution métier conçue pour simplifier la diffusion des emplois du temps au sein de l'établissement. À partir des extractions Excel standards, l'application génère des flux de données au format **iCalendar (.ics)**.

Cet outil permet aux Enseignants Responsables Pédagogiques (ERP) de garantir que les intervenants, enseignants et partenaires disposent d'une visibilité en temps réel sur les plannings, directement intégrée dans leurs outils de travail (Outlook, Teams, Google Calendar, Apple Calendar).

## Fonctionnalités Clés
* **Synchronisation Automatique** : Toute mise à jour du fichier Excel sur la plateforme est instantanément répercutée sur les calendriers des abonnés.
* **Interopérabilité** : Compatibilité totale avec tous les clients de messagerie supportant le protocole iCalendar.
* **Segmentation par Groupe** : Génération distincte de flux pour les groupes P1 et P2 afin de garantir une information ciblée.
* **Historisation** : Suivi en temps réel de la date et de l'heure de la dernière mise à jour pour chaque promotion.

## Guide d'Utilisation pour l'ERP

### 1. Création d'un espace Promotion
Pour chaque nouvelle promotion ou cycle, créez un groupe de calendriers dédié en renseignant le nom de la promotion (ex: FISA 2026) et l'année académique.

### 2. Mise à jour des données
* Sélectionnez la promotion correspondante dans votre tableau de bord.
* Téléversez le fichier Excel d'origine du CESI. Le système traitera automatiquement les cours, intervenants et salles.
* Une fois l'importation terminée, la date de mise à jour est actualisée.

### 3. Diffusion des flux
Chaque groupe dispose d'un lien permanent unique. 
* **Action** : Copiez le lien ICS généré depuis l'interface.
* **Diffusion** : Transmettez ce lien aux intervenants. Il leur suffit de l'ajouter comme "Nouveau calendrier à partir d'un lien" dans leur logiciel habituel pour être abonnés aux changements.

## Accès à la plateforme
L'outil est accessible en ligne à l'adresse suivante :  
👉 **[https://cesi-edt.vercel.app/](https://cesi-edt.vercel.app/)**

---
**Note de confidentialité** : Les données traitées sont exclusivement utilisées pour la génération du planning et restent confinées à l'infrastructure sécurisée de l'application.
