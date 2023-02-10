import { ClientSession, MongoClient } from 'mongodb';

type CollectionData =
    | (Required<{ index: Record<string, unknown> }> & {
          insert?: Record<string, unknown>;
      })
    | (Required<{ insert: Record<string, unknown> }> & {
          index?: Record<string, unknown>;
      });

type Collector = {
    collectionName: string;
    data: CollectionData;
};

type Operation = {
    operationType: OperationType;
    collectors: Collector[];
};

export enum OperationType {
    Create = 'CREATE',
    Update = 'UPDATE',
    Delete = 'DELETE',
}

//TODO: Sample call Transaction
// Transaction.getInstance('dbUri').createTransaction('dbName', [
//     {
//         operationType: OperationType.Create,
//         collectors: [
//             {
//                 collectionName: 'User',
//                 data: {
//                     index: {},
//                     insert: {},
//                 },
//             },
//         ],
//     },
// ]);

export class Transaction {
    private static instance: Transaction;
    private client: MongoClient;

    private constructor(private mongodbUri: string) {
        this.client = new MongoClient(this.mongodbUri);
    }

    /**
     * If the instance is not set, create a new instance and return it
     * @param {string} mongodbUri - The mongodb uri to connect to.
     * @returns The instance of the class.
     */
    public static getInstance(mongodbUri: string): Transaction {
        if (!Transaction.instance)
            Transaction.instance = new Transaction(mongodbUri);

        return Transaction.instance;
    }

    /**
     * It creates a transaction, loops through an array of operations, and performs the operation based
     * on the operation type
     * @param {string} dbName - The name of the database to use.
     * @param {Operation[]} operations - Operation[]
     */
    public async createTransaction(dbName: string, operations: Operation[]) {
        const connection = await this.client.connect();
        const session = connection.startSession();
        session.startTransaction();

        try {
            for await (let operation of operations) {
                switch (operation.operationType) {
                    case OperationType.Create:
                        this.create(dbName, operation.collectors, session);
                        break;
                    case OperationType.Update:
                        this.update(dbName, operation.collectors, session);
                        break;
                    case OperationType.Delete:
                        this.delete(dbName, operation.collectors, session);
                        break;
                }
            }
            await session.commitTransaction();
        } catch (error) {
            await session.abortTransaction();
            throw error;
        }
    }

    /**
     * It creates a database with the given name, and inserts the given data into the given collections
     * @param {string} dbName - The name of the database to create.
     * @param {Collector[]} collectors - An array of objects that contain the collection name and the data
     * to be inserted.
     * @param session - The session object that is passed to the function.
     */
    private async create(
        dbName: string,
        collectors: Collector[],
        session: ClientSession,
    ) {
        const db = this.client.db(dbName);
        for (let collector of collectors) {
            db.collection(collector.collectionName).insertOne(
                { ...collector.data.insert },
                { session },
            );
        }
    }

    /**
     * It takes a database name, a list of collectors, and a session, and updates the database with the
     * data in the collectors
     * @param {string} dbName - The name of the database to update.
     * @param {Collector[]} collectors - An array of objects that contain the data to be inserted into the
     * database.
     * @param session - The session object that was created in the previous step.
     */
    private update(
        dbName: string,
        collectors: Collector[],
        session: ClientSession,
    ) {
        const db = this.client.db(dbName);
        for (let collector of collectors) {
            db.collection(collector.collectionName).updateOne(
                { ...collector.data.index },
                { $set: { ...collector.data.insert } },
                { session },
            );
        }
    }

    /**
     * It deletes a document from a collection in a database
     * @param {string} dbName - The name of the database to delete from.
     * @param {Collector[]} collectors - An array of Collector objects.
     * @param session - The session object that was created in the previous step.
     */
    private delete(
        dbName: string,
        collectors: Collector[],
        session: ClientSession,
    ) {
        const db = this.client.db(dbName);
        for (let collector of collectors) {
            db.collection(collector.collectionName).deleteOne(
                { ...collector.data.index },
                { session },
            );
        }
    }
}
