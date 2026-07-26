
<p align="center">
<img src="readme-assets/hello.png" width="400">
</p>

# CESI-EDT

Plateforme web de gestion des emplois du temps développée pour CESI.

CESI-EDT permet aux **Enseignants Responsables Pédagogiques (ERP)** de gérer les emplois du temps des promotions, de diffuser les calendriers aux enseignants et aux étudiants, ainsi que de suivre différents indicateurs liés à la planification.

Les **enseignants** et les **étudiants** utilisent la plateforme pour consulter leur emploi du temps et s'abonner à leur calendrier personnel.

---

## Accès à la plateforme

La plateforme est accessible à l'adresse suivante :

👉 https://cesi-edt.vercel.app/

Aucune installation n'est nécessaire. Un navigateur web récent suffit.

<p align="center">
<img src="readme-assets/cesi-edt-readme.002.png" width="800">
</p>

---

## Fonctionnalités

### Pour les ERP

- Gestion des promotions
- Import des emplois du temps
- Import des salles depuis les documents FNG
- Consultation et modification des plannings
- Gestion des examens
- Suivi des volumes horaires
- Génération des calendriers ICS
- Export des données
- Tableaux de bord et statistiques

### Pour les enseignants

- Consultation de leur emploi du temps
- Recherche rapide des séances
- Abonnement à leur calendrier (ICS)

### Pour les étudiants

- Consultation de l'emploi du temps de leur promotion
- Abonnement au calendrier (ICS)

---

# Guide d'utilisation pour l'ERP

## 1. Création d'une promotion

Créer une nouvelle promotion en renseignant les informations demandées (nom de la promotion, année académique, cycle, etc.).

Cette promotion servira de conteneur pour l'ensemble des données de planification.

---

## 2. Import de l'emploi du temps

Accéder au module **Import des emplois du temps**.

Importer le fichier Excel, l'emploi du temps normalisé rempli.

Une fois l'import terminé, vérifier que toutes les séances ont bien été créées.

---

## 3. Import des salles

Ouvrir le module **Import des salles**.

Importer les fichiers PDF issus de FNG contenant les affectations des salles.

La plateforme extrait automatiquement les salles et les associe aux séances correspondantes.

---

## 5. Consultation de l'emploi du temps

Utiliser les différents filtres pour afficher :

- une promotion ;
- un groupe ;
- un enseignant ;
- une salle.

Les modifications sont immédiatement visibles.

---

## 6. Gestion des examens

Accéder au module **Examens**.

Créer ou modifier les examens.

Vérifier l'absence de conflits de salles, d'horaires ou d'enseignants.

---

## 7. Suivi des heures

Le tableau de bord permet de consulter :

- les heures prévues ;
- les heures réalisées ;
- les écarts éventuels.

Ces informations peuvent être filtrées par enseignant ou par promotion.

---

## 8. Diffusion des calendriers

Une fois les vérifications terminées, récupérer les liens d'abonnement ICS.

Chaque enseignant et chaque promotion disposent de leur propre calendrier.

Les utilisateurs peuvent ensuite s'abonner à leur calendrier depuis Outlook, Google Calendar, Apple Calendar ou toute autre application compatible.

<p align="center">
<img src="readme-assets/cesi-edt-readme.001.png" width="800">
</p>

---

## 9. Export des données

La plateforme permet d'exporter différents éléments :

- calendriers ICS ;
- tableaux récapitulatifs ;
- statistiques.

---

## Technologies

- Python
- Pandas
- OpenPyXL
- iCalendar (ICS)
- PDFPlumber
- Supabase
- Vercel

---

## Auteur

Projet développé par Jules Hamdan, ERP à CESI Toulouse, pour les équipes pédagogiques de CESI afin de simplifier la gestion des emplois du temps et la diffusion des calendriers.
