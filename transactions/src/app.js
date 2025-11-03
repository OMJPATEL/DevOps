const express = require('express');
const { MongoClient, ObjectId } = require('mongodb');

const app = express();
app.use(express.json());


const PORT = Number(process.env.PORT || 4000);
const MONGO_URI = process.env.MONGO_URI || 'mongodb://mongo:27017/bank_app';


console.log('Starting transactions service...');
console.log('MongoDB URI:', MONGO_URI);
app.use((req, _res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
  next();
});


let db;
(async () => {
  try {
    const client = await MongoClient.connect(MONGO_URI);
    // If URI has no DB name, default to 'bank_app'
    db = client.db() || client.db('bank_app');
    console.log('Successfully connected to MongoDB!');
  } catch (err) {
    console.error('MongoDB connection failed:', err);
    process.exit(1);
  }
})();


const isValidObjectId = (id) => /^[a-fA-F0-9]{24}$/.test(id || '');

// Normalizes date field inside aggregated docs
const normalizeDateStage = {
  $addFields: {
    'transactions.date': {
      $cond: [
        { $eq: [{ $type: '$transactions.date' }, 'string'] },
        { $toDate: '$transactions.date' },
        '$transactions.date',
      ],
    },
  },
};


const groupByMonthStage = {
  $group: {
    _id: {
      year: { $year: '$transactions.date' },
      month: { $month: '$transactions.date' },
    },
    count: { $sum: 1 },
    totalAmount: { $sum: '$transactions.amount' },
    items: {
      $push: {
        type: '$transactions.type',
        amount: '$transactions.amount',
        date: '$transactions.date',
      },
    },
  },
};

const sortDescStage = { $sort: { '_id.year': -1, '_id.month': -1 } };


// Health check
app.get('/status', (_req, res) => res.json({ ok: true }));


app.get('/', async (req, res) => {
  try {
    if (!db) return res.status(503).json({ error: 'DB not ready' });

    const pipeline = [
      { $match: { transactions: { $exists: true, $ne: [] } } },
      { $unwind: '$transactions' },
      normalizeDateStage,
      groupByMonthStage,
      sortDescStage,
    ];

    const data = await db.collection('users').aggregate(pipeline).toArray();
    return res.json(data);
  } catch (err) {
    console.error('Error fetching all transactions:', err);
    return res.status(500).json({ error: 'Failed to load transactions' });
  }
});

app.get('/:userId', async (req, res) => {
  try {
    if (!db) return res.status(503).json({ error: 'DB not ready' });

    const { userId } = req.params;
    if (!isValidObjectId(userId)) {
      return res.status(400).json({ error: 'Invalid user id' });
    }

    const uId = new ObjectId(userId);

 
    const user = await db.collection('users').findOne({ _id: uId }, { projection: { _id: 1 } });
    if (!user) return res.status(404).json({ error: 'User not found' });

    const pipeline = [
      { $match: { _id: uId, transactions: { $exists: true, $ne: [] } } },
      { $unwind: '$transactions' },
      normalizeDateStage,
      groupByMonthStage,
      sortDescStage,
    ];

    const data = await db.collection('users').aggregate(pipeline).toArray();
    return res.json(data);
  } catch (err) {
    console.error('Error fetching user transactions:', err);
    return res.status(500).json({ error: 'Failed to load user transactions' });
  }
});


app.listen(PORT, '0.0.0.0', () => {
  console.log(`Transactions service running on http://0.0.0.0:${PORT}`);
});
