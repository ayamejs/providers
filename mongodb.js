/**
 * @author Raven
 * @license MIT
 *
 * To use this install the mongodb driver `npm install mongodb`
 * And add a connection string to your client options. E.g
 * new AyameClient({ providers: { default: "mongodb", mongodb: "mongodb://stuff..." } })
 */
const { Provider } = require("ayame");
const { MongoClient } = require("mongodb");

class MongoDBProvider extends Provider {
  constructor(...args) {
    super(...args);

    /**
     * The MongoClient connection.
     */
    this.mongoClient = null;

    /**
     * The MongoDB database.
     */
    this.db = null;
  }

  async init() {
    this.mongoClient = await MongoClient.connect(this.client.options.providers.mongodb, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });

    this.db = this.mongoClient.db();
  }

  fetch(table, keys = []) {
    if(keys.length) {
      return this.db.collection(table).find({ id: { $in: keys } }, { projection: { _id: 0 } }).toArray();
    }

    return this.db.collection(table).find({}, { projection: { _id: 0 } }).toArray();
  }

  get(table, id) {
    return this.db.collection(table).findOne({ id }, { projection: { _id: 0 } });
  }
  
  set(table, id, key, value) {
    return this.db.collection(table).update({ id }, { $set: { [key]: value } }, { upsert: true });
  }

  update(table, id, obj) {
    return this.db.collection(table).update({ id }, { $set: obj }, { upsert: true });
  }

  clear(table, id) {
    return this.db.collection(table).deleteOne({ id });
  }

  shutdown() {
    return this.mongoClient.close();
  }
}

module.exports = MongoDBProvider;
