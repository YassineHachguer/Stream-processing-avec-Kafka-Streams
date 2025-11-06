#  Stream processing avec Kafka Streams

[![Java](https://img.shields.io/badge/Java-17-blue?logo=java)](https://www.java.com/)
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

