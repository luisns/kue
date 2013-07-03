"use strict";
/*!
 * kue
 * Copyright (c) 2011 LearnBoost <tj@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter
    , Worker = require('./queue/worker')
    , Events = require('./queue/events')
    , Job = require('./queue/job')
    , redis = require( 'redis' );
/**
 * Expose `Queue`.
 */

exports = module.exports = Queue;

/**
 * Library version.
 */

exports.version = '0.6.5';

/**
 * Expose `Job`.
 */

exports.Job = Job;

/**
 * Server instance (that is lazily required)
 */

var app;

/**
 * Expose the server.
 */

Object.defineProperty(exports, 'app', {
    get: function() {
        return app || (app = require('./http'));
    }
});


/**
 * Create a new `Queue`.
 *
 * @return {Queue}
 * @api public
 */

exports.createQueue = function( connectionOptions, prefix ){
    return new Queue( connectionOptions, prefix );
};

/**
 * Initialize a new job `Queue`.
 *
 * @api public
 */
// FIXME: meanwhile for bw compatibility use client
function Queue( connectionOptions, prefix ) {
    this.connOptions = { port: connectionOptions.port, host: connectionOptions.host };
    this.prefix = prefix || 'q';
    this.client = redis.createClient( this.connOptions.port, this.connOptions.host );
    this.events = new Events( this, this.prefix );

    // pool of worker clients by type
    this.workerClients = {};

    this.timer = null;
}

/**
 * Inherit from `EventEmitter.prototype`.
 */

Queue.prototype.__proto__ = EventEmitter.prototype;

/**
 * Create a `Job` with the given `type` and `data`.
 *
 * @param {String} type
 * @param {Object} data
 * @return {Job}
 * @api public
 */

Queue.prototype.create =
    Queue.prototype.createJob = function(type, data){
        return new Job( this, type, data, this.prefix );
    };

/**
 * Proxy to auto-subscribe to events.
 *
 * @api public
 */

var on = EventEmitter.prototype.on;
Queue.prototype.on = function(event){
    if (0 == event.indexOf('job')) this.events.subscribe();
    return on.apply(this, arguments);
};

/**
 * Promote delayed jobs, checking every `ms`,
 * defaulting to 5 seconds.
 *
 * @params {Number} ms
 * @api public
 */

Queue.prototype.promote = function(ms){
    var self = this;
    var prefix = this.prefix;
    var client = this.client
        , ms = ms || 5000
        , limit = 20;


    // avoid creating another timer if i have one already
    if ( this.timer != null ) {
        return;
    }

    this.timer = setInterval(function(){
        client.sort( prefix + ':jobs:delayed'
            , 'by', prefix + ':job:*->delay'
            , 'get', '#'
            , 'get', prefix + ':job:*->delay'
            , 'get', prefix + ':job:*->created_at'
            , 'limit', 0, limit, function(err, jobs){
                if (err || !jobs.length) return;

                // iterate jobs with [id, delay, created_at]
                while (jobs.length) {
                    var job = jobs.slice(0, 3)
                        , id = parseInt(job[0], 10)
                        , delay = parseInt(job[1], 10)
                        , creation = parseInt(job[2], 10)
                        , promote = ! Math.max(creation + delay - Date.now(), 0);

                    // if it's due for activity
                    // "promote" the job by marking
                    // it as inactive.
                    if (promote) {
                        Job.get( self, id, self.prefix, function(err, job){
                            if (err) return;
                            self.events.emit(id, 'promotion');
                            job.inactive();
                        });
                    }

                    jobs = jobs.slice(3);
                }
            });
    }, ms);
};

/**
 * Stop checking for delayed jobs
 */
Queue.prototype.stopPromotion = function(){
    if ( this.timer != null ) {
        clearInterval( this.timer );
        this.timer = null;
    }
};

/**
 * Get setting `name` and invoke `fn(err, res)`.
 *
 * @param {String} name
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.setting = function(name, fn){
    this.client.hget( this.prefix + ':settings', name, fn);
    return this;
};

/**
 * Process jobs with the given `type`, invoking `fn(job)`.
 *
 * @param {String} type
 * @param {Number|Function} n
 * @param {Function} fn
 * @api public
 */

Queue.prototype.process = function(type, n, fn){
    var self = this;

    if ('function' == typeof n) fn = n, n = 1;

    var workerClient = this.workerClients[type]
        || (this.workerClients[type] = redis.createClient( this.client.port, this.client.host ) );

    while (n--) {
        (function(worker){
            worker.on('error', function(err){
                self.emit('error', err);
            });

            worker.on('job complete', function(job){
                self.client.incrby( self.prefix + ':stats:work-time', job.duration);
            });
        })(new Worker(this, type, workerClient, self.prefix ).start(fn));
    }
};

/**
 * Get the job types present and callback `fn(err, types)`.
 *
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.types = function(fn){
    this.client.smembers( this.prefix + ':job:types', fn);
    return this;
};

/**
 * Return job ids with the given `state`, and callback `fn(err, ids)`.
 *
 * @param {String} state
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.state = function(state, fn){
    this.client.zrange( this.prefix + ':jobs:' + state, 0, -1, fn);
    return this;
};

/**
 * Get queue work time in milliseconds and invoke `fn(err, ms)`.
 *
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.workTime = function(fn){
    this.client.get( this.prefix + ':stats:work-time', function(err, n){
        if (err) return fn(err);
        fn(null, parseInt(n, 10));
    });
    return this;
};

/**
 * Get cardinality of `state` and callback `fn(err, n)`.
 *
 * @param {String} state
 * @param {Function} fn
 * @return {Queue} for chaining
 * @api public
 */

Queue.prototype.card = function(state, fn){
    this.client.zcard( this.prefix + ':jobs:' + state, fn);
    return this;
};

/**
 * Completed jobs.
 */

Queue.prototype.complete = function(fn){
    return this.state('complete', fn);
};

/**
 * Failed jobs.
 */

Queue.prototype.failed = function(fn){
    return this.state('failed', fn);
};

/**
 * Inactive jobs (queued).
 */

Queue.prototype.inactive = function(fn){
    return this.state('inactive', fn);
};

/**
 * Active jobs (mid-process).
 */

Queue.prototype.active = function(fn){
    return this.state('active', fn);
};

/**
 * Completed jobs count.
 */

Queue.prototype.completeCount = function(fn){
    return this.card('complete', fn);
};

/**
 * Failed jobs count.
 */

Queue.prototype.failedCount = function(fn){
    return this.card('failed', fn);
};

/**
 * Inactive jobs (queued) count.
 */

Queue.prototype.inactiveCount = function(fn){
    return this.card('inactive', fn);
};

/**
 * Active jobs (mid-process).
 */

Queue.prototype.activeCount = function(fn){
    return this.card('active', fn);
};

/**
 * Delayed jobs.
 */

Queue.prototype.delayedCount = function(fn){
    return this.card('delayed', fn);
};


/**
 * Removes a job by its id
 */

Queue.prototype.removeJob = function ( id, fn ) {
    Job.get( this, id, this.prefix, function ( err, job ) {
        if ( err ) {
            return fn( err );
        } //means if job has retry set, it will retry

        if ( !job ) {
            fn( new Error("no job found to remove") );
        }
        else {
            job.remove( fn );
        }
    } );
};
