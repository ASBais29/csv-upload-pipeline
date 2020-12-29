const http = require('http');
const fs = require('fs');
const mongodb=require('mongodb')
const csv = require('csv');
const express = require('express');
const multer = require('multer');
const fastcsv = require('fast-csv');
const moment = require('moment');
const async = require('async')
const Router = express.Router;
const upload = multer({ dest: 'tmp/csv/' });
const app = express();
const router = new Router();
const server = http.createServer(app);
const port = 9000

function validateCsvData(rows) {
  const dataRows = rows.slice(1, rows.length); //ignore header at 0 and get rest of the rows
  for (let i = 0; i < dataRows.length; i++) {
    const rowError = validateCsvRow(dataRows[i]);
    if (rowError) {
      return `${rowError} on row ${i + 1}`
    }
  }
  return;
}


function validateCsvRow(row) {
  if (!row[0]) {
    return "invalid name"
  }
  else if (!Number.isInteger(Number(row[1]))) {
    return "invalid roll number"
  }
  else if (!moment(row[2], "YYYY-MM-DD").isValid()) {
    return "invalid date of birth"
  }
  return;
}



//csv files upload to server 9000

router.post('/', upload.single('file'), function (req, res) {
  const fileRows = [];

  // open uploaded file
  fastcsv.fromPath(req.file.path)
    .on("data", function (data) {
      fileRows.push(data); // push each row
    })
    .on("end", function () {
      console.log(fileRows);
      fs.unlinkSync(req.file.path);   // remove temp file

      const validationError = validateCsvData(fileRows);
      if (validationError) {
        return res.status(403).json({ error: validationError });
      }
      //else process "fileRows" and respond
      return res.json({ message: "valid csv" })
    })
});

app.use('/upload-csv', router);

// Start server
function startServer() {
  server.listen(port, function () {
    console.log('Server listening on ', port);
  });
}

setImmediate(startServer);

//csv files upload to Mongo Database

const MongoClient = require('mongodb').MongoClient;

MongoClient.connect('mongodb://localhost:27017', { useUnifiedTopology: true },function(err, client) {
	if (err) throw err;
    var db = client.db('test');
    var collection = db.collection('myCSVs')
    console.log('Connection is established!')
    var queue = async.queue(collection.insert.bind(collection), 100); //for 100K files Upload them in chunks of 100 files

	csv()
	.from.path('./sample.csv', { columns: true })
	.transform(function (data, index, cb) {
		queue.push(data, function (err, res) {
			if (err) return cb(err);
			cb(null, res[0]);
		})
	})
	.on('error', function (err) {
		console.log('ERROR: ' + err.message);
	})
	.on('end', function () {
		queue.drain = function() {
			collection.count(function(err, count) {
				console.log('Number of documents:', count);
				db.close();
			})
		}
	})
})