var Redis = require('ioredis');

module.exports = function (broker) {
  var brokerOptions = broker.options.brokerOptions;
  var instanceId = broker.instanceId;
  
  var subClient = new Redis(brokerOptions.redis);
  var pubClient = new Redis(brokerOptions.redis);

  var publishToCluster = function (channel, data) {
    if (data instanceof Object) {
      try {
        data = '/o:' + JSON.stringify(data);
      } catch (e) {
        data = '/s:' + data;
      }
    } else {
      data = '/s:' + data;
    }
    
    if (instanceId != null) {
      data = instanceId + data;
    }
    
    pubClient.publish(channel, data);
  };

  broker.on('subscribe', subClient.subscribe.bind(subClient));
  broker.on('unsubscribe', subClient.unsubscribe.bind(subClient));
  broker.on('publish', publishToCluster);
  
  var instanceIdRegex = /^[^\/]*\//;
  
  subClient.on('message', function (channel, message) {
    var sender = null;
    message = message.replace(instanceIdRegex, function (match) {
      sender = match.slice(0, -1);
      return '';
    });
    
    // Do not publish if this message was published by 
    // the current SC instance since it has already been
    // handled internally
    if (sender == null || sender != instanceId) {
      var type = message.charAt(0);
      var data;
      if (type == 'o') {
        try {
          data = JSON.parse(message.slice(2));
        } catch (e) {
          data = message.slice(2);
        }
      } else {
        data = message.slice(2);
      }
      broker.publish(channel, data);
    }
  });

  var instance = {};

  instance.publish = function(channel, data) {
    broker.publish(channel, data);
    publishToCluster(channel, data);
  }

  return instance;
};