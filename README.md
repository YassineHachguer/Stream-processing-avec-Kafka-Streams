#  Stream processing avec Kafka Streams

[![Java](https://img.shields.io/badge/Java-21-blue?logo=java)](https://www.java.com/)
[![Kafka](https://img.shields.io/badge/Kafka-3.6.0-orange?logo=apachekafka)](https://kafka.apache.org/)
[![Maven](https://img.shields.io/badge/Maven-3.9.0-red?logo=apachemaven)](https://maven.apache.org/)

---

## Exercice 1: Traitement de messages texte
##  Objectif

Cette application **Kafka Streams** lit des messages texte depuis un topic Kafka `text-input`, les nettoie, et les redirige vers deux topics :

- `text-clean` → messages valides
- `text-dead-letter` → messages invalides

Les messages invalides sont ceux qui sont vides, contiennent des mots interdits (`HACK`, `SPAM`, `XXX`) ou dépassent 100 caractères.

---
## Tests

### Création des topics

```bash
docker exec broker kafka-topics --create --topic text-input --bootstrap-server localhost:9092
docker exec broker kafka-topics --create --topic text-clean --bootstrap-server localhost:9092
docker exec broker kafka-topics --create --topic text-dead-letter --bootstrap-server localhost:9092

```
### Envoi de messages dans text-input
![img.png](images/img.png)

### Vérification des messages valides (text-clean)
![img.png](images/img1.png)

### Vérification des messages invalides (text-dead-letter)
![img.png](images/img2.png)

## Exercice 2 : Analyse de Données Météorologiques

### Objectif

Cette application **Kafka Streams** lit des données météorologiques depuis le topic :

- `weather-data`

Chaque message est au format :
station,temperature,humidity

L’application effectue les traitements suivants :

1. **Lecture** des données météo (KStream)
2. **Filtrage** : conserver uniquement les températures *supérieures à 30°C*
3. **Conversion** : transformation des températures en **Fahrenheit**
4. **Agrégation par station** :
    - calcul de la température moyenne (°F)
    - calcul de l’humidité moyenne (%)
5. **Publication** des résultats agrégés dans le topic :
    - `station-averages`

---

##  Tests

### 1. Création des topics

```bash
docker exec broker kafka-topics --create --topic weather-data --bootstrap-server localhost:9092
docker exec broker kafka-topics --create --topic station-averages --bootstrap-server localhost:9092
```
![img.png](images/img_1.png)
![img.png](images/img_2.png)
![img.png](images/img_3.png)

### 2. Envoi de données dans weather-data
```bash
docker exec -it broker kafka-console-producer --topic weather-data --bootstrap-server localhost:9092
```
![img.png](images/img_4.png)

### 3. Vérification des résultats (topic station-averages)
```bash
docker exec -it broker kafka-console-consumer --topic station-averages --from-beginning --bootstrap-server localhost:9092
```
![img.png](images/img_5.png)

# Exercice 3 : Calcul du nombre de clics avec Kafka Streams et Spring Boot
## Objectif

Développer une solution complète pour **suivre et analyser les clics des utilisateurs en temps réel** :

- Chaque clic utilisateur est envoyé à un topic Kafka.
- Kafka Streams compte dynamiquement le nombre total de clics.
- Les résultats sont exposés via une API REST.

L’objectif est de comprendre le fonctionnement de **Kafka, Kafka Streams** et leur intégration avec **Spring Boot**.

---
## Architecture

1. **Producteur Web** (`Producteur_Web`)
   - Application web Spring Boot avec un bouton "Cliquez ici".
   - Chaque clic envoie un message au topic Kafka `clicks`.
   - Message Kafka : clé = `userId`, valeur = `"click"`.
   - Port recommandé : `8080`.

2. **Application Kafka Streams** (`Application_Kafka_Streams`)
   - Consomme les messages du topic `clicks`.
   - Compte le nombre total de clics par utilisateur ou globalement.
   - Publie les résultats dans le topic Kafka `click-counts`.

3. **Consommateur REST** (`Consommateur_REST`)
   - Consomme les données du topic `click-counts`.
   - Expose un endpoint REST pour récupérer le nombre total de clics :
     ```
     GET /clicks/count
     ```
---

## Tests

### 1. Création des topics Kafka
```bash
docker exec broker kafka-topics --create --topic clicks --bootstrap-server localhost:9092
docker exec broker kafka-topics --create --topic click-counts --bootstrap-server localhost:9092
```
![img.png](images/img_6.png)

### 2. Lancer les applications Spring Boot

- Producteur Web : port 8080

- Application Kafka Streams : pas de serveur web requis, exécuter normalement

- Consommateur REST : port 8081

### 3. Tester le clic
- Avec navigateur : Ouvrir http://localhost:8080/

- Cliquer sur le bouton "Cliquez ici" plusieurs fois
  ![img.png](images/img_7.png)

### 4. Vérification des messages Kafka
```bash
# Vérifier les clics envoyés
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic clicks --from-beginning

# Vérifier le nombre de clics calculé
docker exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic click-counts --from-beginning

```



