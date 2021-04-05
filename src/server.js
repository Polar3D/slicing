/*

Copyright (c) 2017, Polar 3D, LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the <organization> nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL POLAR 3D LLC BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

'use strict';

var PrintJob = require('./db_models/printJob.model');
var Stats = require('./db_models/stats.model');
var lib = require('./lib');
var sqs = require('./lib/sqs');
var logger = require('./lib/logger');
var s3 = require('./lib/s3');
var app = require('express')();
var http = require('http').Server(app);
var path = require('path');
var ld = require('lodash');
var StringTemplateCompile = require('string-template/compile');

var Promise = require('bluebird');
var mongoose = require('mongoose');
mongoose.Promise = Promise;

// Our configuration
var env = process.env.NODE_ENV || 'development';
var config = require('./config/' + env);

// Interface to bind to for TCP connections
var bind_addr = config.ip || '0.0.0.0';
// TCP port to listen on
var bind_port = config.port || 8080;

// Directory for temp files
//   Not using /tmp to avoid compromising swap
var workDir = path.normalize(`${__dirname}/../working`);
s3.setWorkDir(workDir);

// Our cura processing command
var scriptDir = path.normalize(`${__dirname}/../scripts`);
var CuraTemplate = StringTemplateCompile(`${scriptDir}/${config.cura_command}`);

// To spawn a slicer process, we use exec()
//   spawn -- forks a child process with no shell and streams
//              stdin, stdout back.
//   exec  -- forks a child process with a shell and buffers
//              stdin, stdout.  Fine for up to 20K of buffered
//              data.
var exec = require('child-process-promise').exec;

// Our SQS queues
var queue = require('./queue');
queue.setCallback(processMessage);
var queue_prefix = `https://sqs.${config.sqs.awsOptions.region}.amazonaws.com/${config.sqs.account}/`;
queue.setQueueHighPriority(queue_prefix + config.sqs_slicing.queue_high);
queue.setQueueLowPriority(queue_prefix + config.sqs_slicing.queue_low);

// Stats
var jobsSucceeded = 0;
var jobsFailed = 0;
var jobsFailedSlicing = 0;
var jobsCanceled = 0;
var totalSlicingTime = 0.0;

// Each inbound SQS message must have these fields....
var mustHave = [
  'config_file',  // Download URL for the Cura slicing config
  'gcode_file',   // Upload URL for the resulting gcode
  'handle',       // SQS message handle; needed to remove or requeue a SQS message
  'job_id',       // For logging; <serial-number> + '-' + <job-id>
  'job_oid',      // db _id for the print job document
  'stl_file'      // Download URL for the model's STL to slice
];

// Logging test
if (env === 'development') {
  var level = logger.logLevel(logger.DEBUG);
  logger.log(logger.DEBUG, 'Testing logging levels; original logging level is ' + level);
  logger.log(logger.EMERG, function () { return 'EMERG'; });
  logger.log(logger.ALERT, function () { return 'ALERT'; });
  logger.log(logger.CRIT, function () { return 'CRIT'; });
  logger.log(logger.ERROR, function () { return 'ERROR'; });
  logger.log(logger.WARNING, function () { return 'WARNING'; });
  logger.log(logger.NOTICE, function () { return 'NOTICE'; });
  logger.log(logger.INFO, function () { return 'INFO'; });
  logger.log(logger.DEBUG, function () { return 'DEBUG'; });
  logger.log(logger.DEBUG, 'Restoring logging level back to ' + level);
  logger.logLevel(logger.NOTICE);
  logger.log(logger.DEBUG, 'This message should not appear');
  logger.logLevel(level);
}

/**
 *  Connect to MongoDB
 */
mongoose.connect(config.mongo.uri, {safe: true})
  .then(function() {
    logger.log(logger.NOTICE, 'MongoDB connection established');
    // Begin checking the queues
    queue.checkQueues();
    // Set up the periodic loop to renew the invisibility of SQS messages we are processing
    queue.renewMessages();
    return null;
  })
  .catch(function(err) {
    logger.log(logger.CRITICAL, 'MongoDB connection error: ' + err.message);
    throw new Error('Unable to connect to MongoDB; connection error is ' + err.message);
  });

var STATE_WAITING  =  0;
var STATE_PRE      =  1;
var STATE_RUN      =  2;
var STATE_POST     =  3;
var STATE_DONE     =  4;
var STATE_FAIL     = -1;
var STATE_ERR      = -2;

// Update the print job status by updating the print job's mongo db document
function updateState(msg, state, err) {

  var detail, txt, op;

  switch (state) {
    case STATE_WAITING:
      txt = 'Waiting to slice';
      detail = 'Waiting in the slicing queue for the model to be sliced';
      break;

    case STATE_FAIL:
      txt = 'Cannot slice';
      detail = 'The model cannot be sliced; something is incorrect with the STL file';
      break;

    case STATE_ERR:
      txt = 'Error';
      detail = `Error; ${err.message}`;
      break;

    case STATE_PRE:
      txt = 'Preparing slicer';
      detail = 'Preparing to slice the model; downloading the STL file and slicing options';
      break;

    case STATE_RUN:
      txt = 'Slicing';
      detail = 'Slicing the model';
      break;

    case STATE_POST:
      txt = 'Saving sliced model';
      detail = 'Slicing completed; uploading the printing instructions for retrieval by the printer';
      break;

    case STATE_DONE:
      txt = 'Slicing completed';
      detail = 'Slicing process finished; model is ready to print';
      break;

    default:
      logger.log(logger.WARNING, function () {
        return `${msg.job_id}: unknown state sent to updateState; state = ${state}`;
      });
      state = STATE_ERR;
      txt = 'Unknown';
      detail = 'Unknown state';
      break;
  }

  op = {
    $set: {
      slicing: {
        status: state,
        jobID: msg.job_id,
        progress: txt,
        progressDetail: detail,
        downloadTime: msg.download_time,
        uploadTime: msg.upload_time,
        slicingTime: msg.slicing_time
      },
      gcode_file: (state === STATE_DONE) ? msg.gcode_file : 'waiting'
    }
  };

  logger.log(logger.DEBUG, function() {
    return `${msg.job_id}: Changing state to "${txt}"; ${JSON.stringify(op)}`;
  });

  return PrintJob.update({_id: msg.job_oid}, op).exec()
    .then(function(result) {
      if (result && result.n === 0) {
        logger.log(logger.INFO, function() {
          return `${msg.job_id}: Print job ${msg.job_oid} no longer exists; likely removed from the queue`;
        });
        return Promise.reject(new Error('CANCELED'));
      }
      else {
        logger.log(logger.DEBUG, function() {
          return `${msg.job_id}: Updated print job ${msg.job_oid} with new state`;
        });
      }
      return Promise.resolve(msg);
    })
    .catch(function(err) {
      if (err.message === 'CANCELED' || err.message === 'SLICER') {
        // Bump upstairs
        return Promise.reject(err);
      }
      // Don't let the inability to update deter us (may be problem if STATE_DONE state)
      logger.log(logger.WARNING, function() {
        return `${msg.job_id}: Unable to update the print job record; err = ${err.message}`;
      });
      return Promise.resolve(msg);
    });
}

// downloadFiles()
//  - Update the current state to "preparing slicer" (14), and then
//  - Return a promise to download the STL and slicer configuration files
function downloadFiles(msg) {

  // Set state to "preparing slicer"
  return updateState(msg, STATE_PRE)
    .then(function() {

      logger.log(logger.DEBUG, function() {
        return `${msg.job_id}: Downloading from S3 ${msg.stl_key} to local ${msg.stl_local}; ${msg.config_key} to ${msg.config_local}`;
      });

      // Now download the STL and slicer configuration files
      msg.download_time[1] = new Date();
      return Promise.join(s3.downloadObject(msg.job_id, msg.stl_bucket, msg.stl_key, msg.stl_local),
        s3.downloadObject(msg.job_id, msg.config_bucket, msg.config_key, msg.config_local))
        .then(function() {

          msg.download_time[2] = new Date();
          msg.download_time[0] = msg.download_time[2] - msg.download_time[1];

          logger.log(logger.DEBUG, function() {
            return `${msg.job_id}: Finished downloading ${msg.stl_key} to ${msg.stl_local}; ${msg.download_time[0]} ms`;
          });

          // And resolve this promise
          return Promise.resolve(msg);
        });
    });
}

// Spawn the slicer
function spawnSlicer(msg) {

  return updateState(msg, STATE_RUN)
    .then(function() {
      var obj = {
        config: msg.config_local,
        stl: msg.stl_local,
        gcode: msg.gcode_local
      };
      var cmd = CuraTemplate(obj);
      logger.log(logger.DEBUG, function() {
        return `${msg.job_id}: Starting slicer; ${cmd}`;
      });
      msg.slicing_time[1] = new Date();
      return exec(cmd)
        .then(function(res) {
          msg.slicing_time[2] = new Date();
          msg.slicing_time[0] = msg.slicing_time[2] - msg.slicing_time[1];
          totalSlicingTime += msg.slicing_time[0] / 1000.0;  // convert to seconds
          logger.log(logger.DEBUG, function() {
            return `${msg.job_id}: Slicer finished; ${msg.slicing_time[0]} ms; stdout = "${res.stdout}"`;
          });
          return Promise.resolve(msg);
        })
        .catch(function(err) {
          logger.log(logger.WARNING, function() {
            return `${msg.job_id}: Slicer failure; error = ${err.message}`;
          });
          return Promise.reject(new Error('SLICER'));
        });
    });
}

// Upload the gcode file
// - Return a promise to upload the gcode file to S3
function uploadFile(msg) {

  return updateState(msg, STATE_POST)
    .then(function() {

      logger.log(logger.DEBUG, function() {
        return `${msg.job_id}: Uploading from local ${msg.gcode_local} to S3 ${msg.gcode_key}`;
      });

      msg.upload_time[1] = new Date();
      return s3.uploadFile(msg.job_id, msg.gcode_local, msg.gcode_bucket, msg.gcode_key)
        .then(function () {
          msg.upload_time[2] = new Date();
          msg.upload_time[0] = msg.upload_time[2] - msg.upload_time[1];
          logger.log(logger.DEBUG, function() {
            return `${msg.job_id}: Upload finished; ${msg.upload_time[0]} ms`;
          });
          return Promise.resolve(msg);
        });
    });
}

// Remove local files
function cleanFiles(msg) {
  return lib.removeFiles(msg.job_id, [msg.stl_local, msg.config_local, msg.gcode_local])
    .then(function() {
      // terminate the promise chain
      return Promise.resolve(msg);
    });
}

// Notify our cloud services that the message has been processed; that the STL file has been sliced.
function notifyDone(msg) {

  return updateState(msg, STATE_DONE)
    .then(function() {

      // Do not continue to renew visibility
      queue.removeMessage(msg);

      logger.log(logger.INFO, function() {
        return `${msg.job_id}: Sliced in ${msg.slicing_time[0]/1000.0} seconds!`;
      });

      // And move on
      return Promise.resolve(msg);
    });
}

// Process a received SQS message
function processMessage(err, msg) {

  msg = ld.cloneDeep(msg);

  logger.log(logger.DEBUG, function() {
    return `processMessage: msg = ${JSON.stringify(msg)}`;
  });

  msg.job_oid = mongoose.Types.ObjectId(msg.job_oid);
  msg.download_time = [0, 0, 0];
  msg.upload_time = [0, 0, 0];
  msg.slicing_time = [0, 0, 0];

  // Stop now if the message is missing required fields
  var i;
  for (i = 0; i < mustHave.length; i++) {

    if (mustHave[i] in msg) {
      continue;
    }

    // Bad message...
    logger.log(logger.WARNING, function() {
      return 'processMessage: received message lacking the required field ' +
        mustHave[i] + '; msg = ' + JSON.stringify(msg);
    });

    // Reject
    return updateState(msg, STATE_ERR, `Programming error; slicing request is missing the required parameter ${mustHave[i]}`)
      .then(queue.removeMessage)
      .catch(function(err) {
        logger.log(logger.WARNING, function() {
          return `${msg.job_id}: unable to process slicing request AND an error occurred while attempting to delete the request; ${err}`;
        });
        return null;
      });
  }

  // Using the supplied URLs, generate the
  //    S3 bucket names
  //    S3 key names
  //    Local temporary file names

  try {
    lib.parseUrl(msg, workDir, 'stl');
    lib.parseUrl(msg, workDir, 'config');
    lib.parseUrl(msg, workDir, 'gcode');
  }
  catch (e) {
    // Reject
    return updateState(msg, STATE_ERR, `Programming error; invalid data; cannot parse URL; err = ${e.message}`)
      .then(queue.removeMessage)
      .catch(function(e2) {
        logger.log(logger.WARNING, function() {
          return `${msg.job_id}: unable to process slicing request AND an error occurred while attempting to delete the request; ${e2.message}`;
        });
        return null;
      });
  }

  // Track that we have this message in our care
  //   We will periodically renew our holding of it.  While we could just
  //   keep it invisible in the SQS queue for, say, an hour we then run
  //   the risk of having this process die leaving the message untouched
  //   until that hour is up.  That means a user left wondering when their
  //   file will be sliced....  So, instead we only keep it invisible for
  //   a minute at a time and extend the invisibility every 30 seconds or so.
  queue.trackMessage(msg);

  // At this point, consider us as having just added +1 to the count of
  // running processes notifyDone will decrement the count
  queue.runningProcessesInc(1);

  // Now process the message by
  //
  //   1. Update status in db to 'downloading'
  //   2. In parallel
  //      a. Downloading the STL from S3
  //      b. Downloading the slicing config from S3
  //   3. Update status in db to 'slicing'
  //   4. Slice the print
  //   5. Upload the resulting gcode to S3
  //   6. Update the print job with the gcode file info (so we don't reslice if we don't need to)
  //   7. Remove the local files we downloaded or generated
  //   8. Return

  return downloadFiles(msg)
    .then(spawnSlicer)
    .then(uploadFile)
    .then(notifyDone)
    .then(cleanFiles)
    .then(function() {
      // Hourly stats
      Stats.update(
        { _id: lib.objectIdFromTimeStamp() },
        { $inc: { slicing_succeeded: 1, slicing_seconds: msg.slicing_time[0]/1000.0 } },
        { upsert: true, setDefaultsOnInsert: true }).exec()
        .then(function() { return null; })
        .catch(function(err) {
          logger.log(logger.WARNING, function() {
            return `${msg.job.id}: Failed to update slicing stats; ${err.message}`;
          });
          return null;
        });
      // Lifetime stats
      Stats.update(
        { _id: lib.objectIdTimeZero },
        { $inc: { slicing_succeeded: 1, slicing_seconds: msg.slicing_time[0]/1000.0 } },
        { upsert: true, setDefaultsOnInsert: true }).exec()
        .then(function() { return null; })
        .catch(function(err) {
          logger.log(logger.WARNING, function() {
            return `${msg.job.id}: Failed to update slicing stats; ${err.message}`;
          });
          return null;
        });
      jobsSucceeded += 1;
      return null;
    })
    .catch(function(err) {
      var requeue;
      if (err.message === 'CANCELED') {
        // Job was removed from the queue...
        logger.log(logger.INFO, function () {
          return `${msg.job_id}: Slicing canceled; job appears to have been canceled`;
        });
        requeue = false;
        jobsCanceled += 1;
        Stats.update(
          { _id: lib.objectIdFromTimeStamp() },
          { $inc: { slicing_canceled: 1 } },
          { upsert: true, setDefaultsOnInsert: true }).exec()
          .then(function() { return null; })
          .catch(function(err) {
            logger.log(logger.WARNING, function() {
              return `${msg.job.id}: Failed to update slicing stats; ${err.message}`;
            });
            return null;
          });
        // Cannot readily update the job state -- it's in the completed_jobs collection
      }
      else if (err.message === 'SLICER') {
        // Model will not slice
        logger.log(logger.INFO, function() {
          return `${msg.job_id}: Slicing canceled; model fails to slice`;
        });
        requeue = false;
        jobsFailedSlicing += 1;
        Stats.update(
          { _id: lib.objectIdFromTimeStamp() },
          { $inc: { slicing_failed: 1 } },
          { upsert: true, setDefaultsOnInsert: true }).exec()
          .then(function() { return null; })
          .catch(function(err) {
            logger.log(logger.WARNING, function() {
              return `${msg.job.id}: Failed to update slicing stats; ${err.message}`;
            });
            return null;
          });
        updateState(msg, STATE_FAIL).then(function() { return null; }).catch(function() { return null; });
      }
      else {
        requeue = true;
        jobsFailed += 1;
        updateState(msg, STATE_ERR, err)
          .then(function () { return null; })
          .catch(function () { return null; });
      }

      // Clean up temporary files
      lib.removeFiles(msg.job_id, [msg.stl_local, msg.config_local, msg.gcode_local])
        .then(function() { return null; })
        .catch(function() { return null; });

      // Must be after removeFiles()
      if (requeue) {
        queue.requeueMessage(msg);
      }
      else {
        queue.removeMessage(msg);
      }
      return null;
    });
}

/**
 *  For pinging from monitoring stations, load balancers, etc.
 */
app.get('/info', function(req, res) {
  return res.status(200).send((new Date()).toISOString().replace(/T/, ' ').replace(/\..+/, ''));
});

app.get('/stats', function(req, res) {
  return res.status(200).json(ld.merge(
    {
      jobsSucceeded: jobsSucceeded,
      jobsFailed: jobsFailed,
      jobsFailedSlicing: jobsFailedSlicing,
      jobsCanceled: jobsCanceled,
      totalSlicingTime: totalSlicingTime
    },
    queue.stats()));
});

http.listen(bind_port, bind_addr, function () {
  var addr = '*';
  if (bind_addr !== '0.0.0.0') addr = bind_addr;
  logger.log(logger.NOTICE, 'listening on ' + bind_addr + ':' + bind_port);

  // Now that we're bound and listening, fall back to non-root UID and GIDs
  if (!ld.isEmpty(config.perms)) {
    logger.log(logger.DEBUG, 'Changing uid:gid to ' + config.perms.uid + ':' + config.perms.gid);
    try {
      process.setgroups([config.perms.gid]);
      process.setgid(config.perms.gid);
      process.setuid(config.perms.uid);
      logger.log(logger.NOTICE, 'Changed uid:gid to '+ config.perms.uid + ':' + config.perms.gid);
    }
    catch (err) {
      throw new Error('Failed to change uid and gid; ' + JSON.stringify(err));
    }
  }
  else {
    logger.log(logger.NOTICE, 'Leaving uid and gid unchanged');
  }
});
