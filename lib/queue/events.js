"use strict";
/*!
 * kue - events
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */
var redis = require( 'redis' );

/**
 *
 * @param queue
 * @constructor
 */
function Events( queue, prefix ) {
    this.queue = queue;
    this.pubClient = redis.createClient( queue.client.port, queue.client.host) ;
    this.subClient = redis.createClient( queue.client.port, queue.client.host) ;
    /**
     * Job map.
     */
    this.jobs = {};
    /**
     * Pub/sub key.
     */
    this.key = prefix + ':events';
}
/**
 * Add `job` to the jobs map, used
 * to grab the in-process object
 * so we can emit relative events.
 *
 * @param {Job} job
 * @api private
 */

Events.prototype.add = function(job){
    if (job.id) this.jobs[job.id] = job;
    if (!this.subscribed) this.subscribe();
};

/**
 * Subscribe to "q:events".
 *
 * @api private
 */

Events.prototype.subscribe = function(){
    if (this.subscribed) return;
    var client = this.subClient;
    client.subscribe(this.key);
    client.on('message', this.onMessage.bind(this));
    this.subscribed = true;
};

/**
 * Message handler.
 *
 * @api private
 */

Events.prototype.onMessage = function(channel, msg){
    // TODO: only subscribe on {Queue,Job}#on()
    var msg = JSON.parse(msg);

    // map to Job when in-process
    var job = this.jobs[msg.id];
    if (job) {
        job.emit.apply(job, msg.args);
        // TODO: abstract this out
        if ('progress' != msg.event) delete this.jobs[job.id];
    }

    // emit args on Queues
    msg.args[0] = 'job ' + msg.args[0];
    msg.args.push(msg.id);
    this.queue.emit.apply(this.queue, msg.args);
};

/**
 * Emit `event` for for job `id` with variable args.
 *
 * @param {Number} id
 * @param {String} event
 * @param {Mixed} ...
 * @api private
 */

Events.prototype.emit = function(id, event) {
    var client = this.pubClient
        , msg = JSON.stringify({
            id: id
            , event: event
            , args: [].slice.call(arguments, 1)
        });
    client.publish(this.key, msg);
};


module.exports = Events;