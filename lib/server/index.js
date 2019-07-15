'use strict';

var engine = require('engine.io');
var Promise = require('bluebird');
var uuid = require('../shared/uuid');
var errors = require('../shared/errors');
var utils = require('../shared/utils');
var serverUtils = require('./utils');
var safeEval = require('./safe-eval');
var makePouchCreator = require('./make-pouch-creator');
var dbs = {};
var allChanges = {};

var log = require('debug')('pouchdb:socket:server');

function destringifyArgs(argsString) {
  var args = JSON.parse(argsString);
  var funcArgs = ['filter', 'map', 'reduce'];
  args.forEach(function (arg) {
    if (typeof arg === 'object' && arg !== null && !Array.isArray(arg)) {
      funcArgs.forEach(function (funcArg) {
        if (typeof arg[funcArg] === 'undefined' || arg[funcArg] === null) {
          delete arg[funcArg];
        } else if (arg[funcArg].type === 'func' && arg[funcArg].func) {
          arg[funcArg] = safeEval(arg[funcArg].func);
        }
      });
    }
  });
  return args;
}

function sendUncaughtError(socket, data, options) {
  log(' -> sendUncaughtError', socket.id, data);
  if (options.topic) {
    socket.emit(options.topic, 'global:4:' + JSON.stringify(serverUtils.createError(data)));
  } else {
    socket.send('global:4:' + JSON.stringify(serverUtils.createError(data)));
  }
}

function sendError(socket, messageId, data, options) {
  log(' -> sendError', socket.id, messageId, data);
  if (options.topic) {
    socket.emit(options.topic, messageId + ':0:' + JSON.stringify(serverUtils.createError(data)));
  } else {
    socket.send(messageId + ':0:' + JSON.stringify(serverUtils.createError(data)));
  }
}

function sendSuccess(socket, messageId, data, options) {
  log(' -> sendSuccess', socket.id, messageId);
  if (options.topic) {
    socket.emit(options.topic, messageId + ':1:' + JSON.stringify(data));
  } else {
    socket.send(messageId + ':1:' + JSON.stringify(data));
  }
}

function sendBinarySuccess(socket, messageId, type, buff, options) {
  log(' -> sendBinarySuccess', socket.id, messageId);
  var blobUuid = uuid();
  if (options.topic) {
    socket.emit(options.topic, messageId + ':3:' + JSON.stringify({type: type, uuid: blobUuid}));
    socket.emit(options.topic, Buffer.concat([
      new Buffer(blobUuid, 'utf8'),
      buff]));
  } else {
    socket.send(messageId + ':3:' + JSON.stringify({type: type, uuid: blobUuid}));
    socket.send(Buffer.concat([
      new Buffer(blobUuid, 'utf8'),
      buff]));
  }
}

function sendUpdate(socket, messageId, data, options) {
  log(' -> sendUpdate', socket.id, messageId);
  if (options.topic) {
    socket.emit(options.topic, messageId + ':2:' + JSON.stringify(data));
  } else {
    socket.send(messageId + ':2:' + JSON.stringify(data));
  }
}

function getKey(socket, options) {
  if (options.topic) {
    return '$' + socket.id + '_' + options.topic;
  } else {
    return '$' + socket.id;
  }
}

function dbMethod(socket, methodName, messageId, args, options) {
  var db = dbs[ getKey(socket, options) ];
  if (!db) {
    return sendError(socket, messageId, {error: 'db not found'}, options);
  }
  Promise.resolve().then(function () {
    return db;
  }).then(function (res) {
    var db = res.pouch;
    return db[methodName].apply(db, args);
  }).then(function (res) {
    sendSuccess(socket, messageId, res, options);
  }).catch(function (err) {
    sendError(socket, messageId, err, options);
  });
}

function changes(socket, messageId, args, options) {
  var opts = args[0];
  if (opts && typeof opts === 'object') {
    // just send all the docs anyway because we need to emit change events
    // TODO: be smarter about emitting changes without building up an array
    opts.returnDocs = true;
    opts.return_docs = true;
    // just send binary as base64 and decode on the client
    opts.binary = false;
  }
  dbMethod(socket, 'changes', messageId, args, options);
}

function possiblyBinaryDbMethod(socket, methodName, messageId, args, options) {
  var opts = args[args.length - 1];
  if (opts && typeof opts === 'object') {
    // just send binary as base64 and decode on the client
    opts.binary = false;
  }
  dbMethod(socket, methodName, messageId, args, options);
}

function getAttachment(socket, messageId, args, options) {
  var db = dbs[ getKey(socket, options) ];
  if (!db) {
    return sendError(socket, messageId, {error: 'db not found'}, options);
  }

  Promise.resolve().then(function () {
    return db;
  }).then(function (res) {
    var db = res.pouch;
    var docId = args[0];
    var attId = args[1];
    var opts = args[2];
    if (typeof opts !== 'object') {
      opts = {};
    }
    return db.get(docId, opts).then(function (doc) {
      if (!doc._attachments || !doc._attachments[attId]) {
        throw errors.MISSING_DOC;
      }
      var type = doc._attachments[attId].content_type;
      return db.getAttachment.apply(db, args).then(function (buff) {
        sendBinarySuccess(socket, messageId, type, buff, options);
      });
    });
  }).catch(function (err) {
    sendError(socket, messageId, err, options);
  });
}

function destroy(socket, messageId, args, options) {
  var key = getKey(socket, options);
  var db = dbs[key];
  if (!db) {
    return sendError(socket, messageId, {error: 'db not found'}, options);
  }
  delete dbs[key];

  Promise.resolve().then(function () {
    return db;
  }).then(function (res) {
    var db = res.pouch;
    return db.destroy.apply(db, args);
  }).then(function (res) {
    sendSuccess(socket, messageId, res, options);
  }).catch(function (err) {
    sendError(socket, messageId, err, options);
  });
}

function liveChanges(socket, messageId, args, options) {
  var db = dbs[getKey(socket, options)];
  if (!db) {
    return sendError(socket, messageId, {error: 'db not found'}, options);
  }
  log('liveChanges key ' + getKey(socket, options));
  // log('liveChanges db ' + db);
  Promise.resolve().then(function () {
    return db;
  }).then(function (res) {
    var localOptions = Object.assign({}, options)
    var db = res.pouch;
    var opts = args[0] || {};
    // just send binary as base64 and decode on the client
    opts.binary = false;
    var changes = db.changes(opts);
    allChanges[messageId] = changes;
    changes.on('change', function (change) {
      sendUpdate(socket, messageId, change, localOptions);
    }).on('complete', function (change) {
      changes.removeAllListeners();
      delete allChanges[messageId];
      sendSuccess(socket, messageId, change, localOptions);
    }).on('error', function (change) {
      changes.removeAllListeners();
      delete allChanges[messageId];
      sendError(socket, messageId, change, localOptions);
    });
  });
}

function cancelChanges(messageId) {
  var changes = allChanges[messageId];
  if (changes) {
    changes.cancel();
  }
}

function addUncaughtErrorHandler(db, socket, options) {
  return db.then(function (res) {
    res.pouch.on('error', function (err) {
      log('ERROR! ' + err);
      sendUncaughtError(socket, err, options, options);
    });
  });
}

function createDatabase(socket, messageId, args, pouchCreator, options) {
  var name = typeof args[0] === 'string' ? args[0] : args[0].name;

  var key = getKey(socket, { topic: name });
  log('createDatabase key ' + key);
  var db = dbs[key];
  if (db) {
    return sendError(socket, messageId, {
      error: "file_exists",
      reason: "The database could not be created, the file already exists."
    }, options);
  }

  if (!name) {
    return sendError(socket, messageId, {
      error: 'you must provide a database name'
    }, options);
  }
  db = dbs[key] = pouchCreator(args, socket);
  var topic = name;
  options.topic = topic

  var localOptions = Object.assign({}, options)

  socket.on(topic, function (message) {
    log('TOP! ' + topic + ' ' + message)
    options.topic = topic
    if (typeof message !== 'string') {
      return onReceiveBinaryMessage(message, socket, localOptions);
    }
    onReceiveTextMessage(message, socket, pouchCreator, localOptions);
  })

  addUncaughtErrorHandler(db, socket, options).then(function () {
    sendSuccess(socket, messageId, {ok: true}, localOptions);
  }).catch(function (err) {
    sendError(socket, messageId, err, localOptions);
  });
}

function onReceiveMessage(socket, type, messageId, args, pouchCreator, options) {
  log('onReceiveMessage', type, socket.id, messageId, args);
  switch (type) {
    case 'createDatabase':
      return createDatabase(socket, messageId, args, pouchCreator, options);
    case 'id':
      sendSuccess(socket, messageId, socket.id, options);
      return;
    case 'info':
    case 'put':
    case 'bulkDocs':
    case 'post':
    case 'remove':
    case 'revsDiff':
    case 'compact':
    case 'viewCleanup':
    case 'removeAttachment':
    case 'putAttachment':
      return dbMethod(socket, type, messageId, args, options);
    case 'get':
    case 'query':
    case 'allDocs':
      return possiblyBinaryDbMethod(socket, type, messageId, args, options);
    case 'changes':
      return changes(socket, messageId, args, options);
    case 'getAttachment':
      return getAttachment(socket, messageId, args, options);
    case 'liveChanges':
      return liveChanges(socket, messageId, args, options);
    case 'cancelChanges':
      return cancelChanges(messageId, options);
    case 'destroy':
      return destroy(socket, messageId, args, options);
    default:
      return sendError(socket, messageId, {error: 'unknown API method: ' + type}, options);
  }
}

function onReceiveTextMessage(message, socket, pouchCreator, options) {
  try {
    var split = utils.parseMessage(message, 3);
    var type = split[0];
    var messageId = split[1];
    var args = destringifyArgs(split[2]);
    onReceiveMessage(socket, type, messageId, args, pouchCreator, options);
  } catch (err) {
    log('invalid message, ignoring', err);
  }
}

function onReceiveBinaryMessage(message, socket, options) {
  try {
    var headerLen = parseInt(message.slice(0, 16).toString('utf8'), 10);
    var header = JSON.parse(message.slice(16, 16 + headerLen).toString('utf8'));
    var body = message.slice(16 + headerLen);
    header.args[header.blobIndex] = body;
    onReceiveMessage(socket, header.messageType, header.messageId, header.args, options);
  } catch (err) {
    log('invalid message, ignoring', err);
  }
}

function listen(port, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }
  options = options || {};
  var server = engine.listen(port, options.socketOptions || {}, callback);

  var pouchCreator = makePouchCreator(options);

  var localOptions = Object.assign({}, options)

  server.on('connection', function(socket) {
    socket.on('message', function (message) {
      if (typeof message !== 'string') {
        return onReceiveBinaryMessage(message, socket, localOptions);
      }
      onReceiveTextMessage(message, socket, pouchCreator, localOptions);
    }).on('close', function () {
      log('closing socket', socket.id);
      socket.removeAllListeners();
      delete dbs[ getKey(socket, localOptions) ];
    }).on('error', function (err) {
      log('socket threw an error', err);
      socket.removeAllListeners();
      delete dbs[ getKey(socket, localOptions) ];
    });
  });
}

function piggy(socket, options, callback) {
  console.log('PIGGY!')
  log('piggy! ' + options)
  var pouchCreator = makePouchCreator(options, socket);
  var localOptions = Object.assign({}, options)
  var topic = options.topic

  if (topic) {
    socket.on('pouch', function (message) {
      // used for global commands like createDB
      log('POUCH! ' + message)
      onReceiveTextMessage(message, socket, pouchCreator, localOptions);
    })
  } else {
    socket.on('message', function (message) {
      if (typeof message !== 'string') {
        return onReceiveBinaryMessage(message, socket, localOptions);
      }
      onReceiveTextMessage(message, socket, pouchCreator, localOptions);
    })
  }
  socket.on('close', function () {
    log('closing socket', socket.id);
    socket.removeAllListeners();
    delete dbs[ getKey(socket, localOptions) ];
  }).on('error', function (err) {
    log('socket threw an error', err);
    socket.removeAllListeners();
    delete dbs[ getKey(socket, localOptions) ];
  });
}

module.exports = {
  listen: listen,
  piggy: piggy,
  onReceiveBinaryMessage: onReceiveBinaryMessage,
  onReceiveTextMessage: onReceiveTextMessage
};