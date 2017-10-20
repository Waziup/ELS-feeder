"use strict";

const log = require('./log');
const config = require('./config');
var Task = require('./task.js');

const delay = 500; // time between requests at start (in ms)
const tasks = {};

async function feedData(taskCid, data) {
    log.info('feedData ')
    //log.info(taskCid, data);
    const task = tasks[taskCid];
    const sensors = await task.filterSensors(data);
    await task.feedToElasticsearch(sensors);
}

async function run() {
    log.info('feeder:run ');
    
    const taskConfs = config.get('tasks');

    for (const conf of taskConfs) {
        const task = new Task(conf);
        await task.init();
        tasks[task.cid] = task;
    }

    let accumulatedDelay = 0;
    for (const taskCid in tasks) {
        const task = tasks[taskCid];
        setTimeout(async () => {
            await task.doPeriod();
            if (task.conf.period) {
                setInterval(() => task.doPeriod(), task.conf.period);
            }
        }, accumulatedDelay);

        accumulatedDelay += delay;
    }
}

module.exports.run = run;
module.exports.feedData = feedData;