const http = require('http');
const fs = require('fs');
const mongodb=require('mongodb')
const csv = require('csv');
const express = require('express');
const multer = require('multer');
const fastcsv = require('fast-csv');
const moment = require('moment');
const async = require('async')
const spawn = require('child_process').spawn

const Router = express.Router;
const upload = multer({ dest: 'tmp/csv/' });
const app = express();
const router = new Router();
const server = http.createServer(app);
const port = 9000

function validateData(rows) {
  const dataRows = rows.slice(1, rows.length); //ignore header at 0
  for (let i = 0; i < dataRows.length; i++) {
    const rowError = validateCsvRow(dataRows[i]);
    if (rowError) {
      return `${rowError} on row ${i + 1}`
    }
  }
  return;
}


function validateRow(row) {
  if (!row[0]) {
    return "invalid name"
  }
  else if (!Number.isInteger(Number(row[1]))  || Number(row[1]) !=4012982131) {   // Task 3: Validate data if mobile numbers are mapped wrongly, kill upload
    return "invalid mobile"
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


// terminate long-running tasks


var job = null //keeping the job in memory to kill it

app.get('/save', function(req, res) {

    if(job && job.pid)
        return res.status(500).send('Job is already running').end()

    job = spawn('node', ['/path/to/save/job.js'], 
    {
        detached: false, 
        stdio: [process.stdin, process.stdout, process.stderr] 
    })

    job.on('close', function(code) { 
        job = null 
        //send socket informations about the job ending
    })

    return res.status(201) //created
})

app.get('/stop', function(req, res) {
    if(!job || !job.pid)
        return res.status(404).end()

    job.kill('SIGTERM')
    //or process.kill(job.pid, 'SIGTERM')
    job = null
    return res.status(200).end()
})

app.get('/isAlive', function(req, res) {
    try {
        job.kill(0)
        return res.status(200).end()
    } catch(e) { return res.status(500).send(e).end() }
})