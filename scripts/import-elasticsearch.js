const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'

// Création de l'indice
client.indices.create({
    index: heroesIndexName,
    body: {
        mappings : {
            properties : {
                suggest : { "type" : "completion" }
            }
        }
    }
}, (err, resp) => {
    if (err) console.trace(err.message);
});

let heroes = []

    // Read CSV file
    fs.createReadStream('scripts/all-heroes.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => {
                hereos.push({
                    "name": data.name,
                    "aliases":data["aliases"].split(','),
                    "secretIdentity":data.secretIdentity.split(','),
                    "universe":data.universe,
                    "firstAppearance":data.firstAppearance,
                    "yearAppearance":data.yearAppearance,
                    "powers":data.powers.split(','),
                    "suggest": [ {
                        "input":data.name,
                        "weight":10
                    }, {
                        "input":data.aliases.split(","),
                        "weight":5
                    }, {
                        "input":data.secretIdentity.split(","),
                        "weight":7
                    }]
                })
               
            if (hereos.length / 20000 >= 1) {
                client.bulk(createBulkInsertQuery(heroes), (err, resp) => {
                    if (err) console.trace(err.message);
                    else console.log(`Inserted ${resp.body.items.length} heroes`);
                    client.close();
                    console.log('Terminated!');
                });
                heroes = [];
            }
        })
        .on('end', () => {
            client.bulk(createBulkInsertQuery(heroes), (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} heroes`);
                client.close();
                console.log('Terminated!');
            });
        });


// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(heroes) {
    const body = hereos.reduce((acc, heroes) => {
      const { 
        name,
        aliases,
        secretIdentity,
        universe,
        firstAppearance,
        yearAppearance,
        powers,
        suggest
    } = heroe;
      acc.push({ index: { _index: heroesIndexName, _type: '_doc' } })
      acc.push({ name,
        aliases,
        secretIdentity,
        universe,
        firstAppearance,
        yearAppearance,
        powers,
        suggest })
      return acc
    }, []);
  
    return { body };
  }

run().catch(console.error);
