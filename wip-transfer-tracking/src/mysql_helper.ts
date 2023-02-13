const db = require("./mysqldb2"); 

export default {
    getLastBlockNumber: async (req, res) => {
       let queryString = `SELECT source_block FROM onchain_token_transfers where chain_id = 2 ORDER BY source_time DESC limit 1`;  
       const [source_block] = await db.query(queryString).catch(err => {throw err}); 
       res.json(source_block); 
    }
   };