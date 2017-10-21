"use strict";

const Promise = require('bluebird');
const express = require('express');
const bodyParser = require('body-parser');
const config = require('config');
const log = require('./log');
const helpers = require('./helpers');
const feeder = require('./feeder');


const router = express.Router();
router.post('/update/:cid', helpers.safeHandler(async (req, res) => {
    log.info('Received data from Orion ')
    log.info('req.params.cid', req.params.cid);
    log.info('Req.body.data', req.body.data);
    await feeder.feedData(req.params.cid, req.body.data);
    res.end();
}));

const app = express();

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

app.use('/api', router);

async function run() {
    await new Promise(resolve => app.listen(config.get('endpoint.port'), config.get('endpoint.host'), () => resolve()));
    log.info('Listening on port ', config.get('endpoint.port'));
}

module.exports.run = run;