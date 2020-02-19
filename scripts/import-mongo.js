var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

const insertActors = (db, callback) => {
    const collection = db.collection(collectionName);

    const heroes = [];
    fs.createReadStream('all-heroes.csv')
        .pipe(csv())
        // Pour chaque ligne on créé un document JSON pour l'acteur correspondant
        .on('data', data => {
            heroes.push({
                "id": data.id,
                "name": data.name,
                "description": data.description,
                "imageUrl": data.imageUrl,
                "backgroundImageUrl": data.backgroundImageUrl,
                "externalLink": data.externalLink,
                "identity": {
                    "secretIdentities": data.secretIdentities.split(","),
                    "birthPlace": data.birthPlace,
                    "occupation": data.occupation,
                    "aliases": data.aliases.split(","),
                    "alignment": data.alignment,
                    "firstAppearance": data.firstAppearance,
                    "yearAppearance": parseInt(data.yearAppearance),
                    "universe": data.universe
                },
                "appearance": {
                    "gender": data.gender,
                    "race": data.race,
                    "type": data.type,
                    "height": parseInt(data.height),
                    "weight": parseInt(data.weight),
                    "eyeColor": data.eyeColor,
                    "hairColor": data.hairColor
                },
                "teams": data.teams.split(','),
                "powers": data.powers.split(","),
                "partners": data.partners.split(","),
                "skills": {
                    "intelligence": parseInt(data.intelligence),
                    "strength": parseInt(data.strength),
                    "speed": parseInt(data.speed),
                    "durability": parseInt(data.durability),
                    "power": parseInt(data.power),
                    "combat": parseInt(data.combat)
                },
                "creators": data.creators.split(",")
            });
        })
        // A la fin on créé l'ensemble des acteurs dans MongoDB
        .on('end', () => {
            collection.insertMany(heroes, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, client) => {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertActors(db, result => {
        console.log(`${result.insertedCount} heroes inserted`);
        client.close();
    });
});