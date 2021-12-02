const { Kafka, CompressionTypes, logLevel } = require('kafkajs');

//
// Class manages a kafka connection and produces kafka messages based on
// Volante events.
//
module.exports = {
  name: 'VolanteKafka',
  props: {
    enabled: true,                      // flag to disable auto-init
    brokers: ['kafka-headless:9092'],   // array of brokers into kafka cluster
    compression: CompressionTypes.GZIP, // compression to use for published messages, uses kafkajs types
    groupId: null,                      // specify groupId, default: volante hub name will be used
    clientId: null,                     // specify clientId, default: volante hub name + hostname
    countLogInterval: 10000,            // interval in ms at which to log msg counts
    publishStar: false,                 // subscribe to all volante events and publish them to kafka
    publishStarTopic: 'volante',        // topic to use for publishing all volante events
  },
  stats: {
    totalPublishedMessages: 0,
    totalReceivedMessages: 0,
    numSubscriptions: 0,
  },
  init() {
    if (this.configProps && this.enabled) {
      this.initialize();
    }
    // set up counter logging timer
    setInterval(this.logCounts, this.countLogInterval);
  },
  data() {
    return {
      kafka: null,
      admin: null,
      producer: null,
      consumer: null,
      intervalPublishedMessages: 0,
      intervalReceivedMessages: 0,
    };
  },
  events: {
    'VolanteKafka.start'() {
      this.initialize();
    },
    'VolanteKafka.publish'(topic, msg) {
      this.publish(...arguments);
    },
    'VolanteKafka.subscribe'(topic, callback) {
      this.subscribe(topic, callback);
    },
  },
  updated() {
    if (this.publishStar) {
      this.$warn('subcribing to all volante events');
      // this.$hub.onAll(this.name, (...arguments) => {
      //   console.log(arguments)
      // });
    }
  },
  methods: {
    initialize() {
      // default the groupId and clientId if they werent specified
      this.groupId = this.groupId || this.$hub.name; // use the hub name for id
      // append the hostname to identify this instance 
      this.clientId = this.clientId || `${this.$hub.name}-${require('os').hostname()}`;

      try {
        this.$log(`setting up kafka brokers: ${this.brokers}`);
        this.kafka = new Kafka({
          logLevel: logLevel.NOTHING,
          clientId: this.clientId,
          brokers: this.brokers,
          connectionTimeout: 3000,
          retry: {
            initialRetryTime: 100,
            retries: 10,
          },
        });
        // create producer
        this.producer = this.kafka.producer();
        this.producer.connect().catch((e) => {
          this.$error('Kafka producer can\'t connect to broker', e.name, e);
        });
        this.producer.on(this.producer.events.CONNECT, () => {
          this.$ready(`Producer connected to Kafka at ${this.brokers}`);
        });
      } catch (e) {
        this.$error('error initializing kafkajs', e);
      }
    },
    //
    // publish a message to Kafka
    //
    publish(topic, msg, callback) {
      this.$isDebug && this.$debug(`publishing message with length ${msg.length} to ${topic}`);
      this.producer.send({
        topic,
        compression: this.compression,
        messages: [{ value: msg }],
      }).then(() => {
        this.intervalPublishedMessages++;
        this.totalPublishedMessages++;
        callback && callback(null);
      }).catch((e) => {
        this.$warn(e);
        callback && callback(e);
      });
    },
    //
    // method for subscribing to Kafka topic, creates a new consumer, subscribes it, and calls
    // the given callback with each message
    //
    async subscribe(topic, callback) {
      if (this.kafka && callback) {
        this.$log(`subscribing to topic: ${topic} with groupId: ${this.groupId}`);
        this.numSubscriptions++;
        // create new consumer for this topic
        const consumer = this.kafka.consumer({ groupId: this.groupId });
        await consumer.connect();
        await consumer.subscribe({ topic });
        await consumer.run({
          eachMessage: (msg) => {
            this.intervalReceivedMessages++;
            this.totalReceivedMessages++;
            callback(msg);
          },
        });
      } else {
        this.$warn('no kafka to take this subscription or no callback provided');
      }
    },
    //
    // periodically log the message counts
    //
    logCounts() {
      this.$log(`published: ${this.intervalPublishedMessages}|received: ${this.intervalReceivedMessages}`);
      this.intervalPublishedMessages = 0;
      this.intervalReceivedMessages = 0;
    },
  },
};

// standalone/test code
if (require.main === module) {
  console.log('running test volante wheel');
  const volante = require('volante');

  let hub = new volante.Hub().debug();
  hub.attachAll().attachFromObject(module.exports);
  
  if (process.env.volante_VolanteKafka_brokers) {
    hub.emit('VolanteKafka.update', {
      brokers: [process.env.volante_VolanteKafka_brokers],
      publishStar: true,
    });
  }
  
  hub.emit('VolanteKafka.start');
  
  hub.on('VolanteKafka.ready', () => {
    hub.emit('VolanteKafka.subscribe', 'test', (obj) => {
      console.log(`received message with offset: ${obj.message.offset} - value length: ${obj.message.value.length}`);
    });
    hub.emit('VolanteKafka.publish', 'test', 'test string');
    // hub.emit('VolanteKafka.publish', 'test', require('crypto').randomBytes(100000000));
  });
}
