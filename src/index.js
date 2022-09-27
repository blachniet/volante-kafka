const { Kafka, CompressionTypes, logLevel } = require('kafkajs');

//
// Kakfa for Volante
//
module.exports = {
  name: 'VolanteKafka',
  props: {
    enabled: true,                      // flag to disable auto-init with volante config
    brokers: ['kafka-headless:9092'],   // array of brokers (host:port) into kafka cluster
    compression: CompressionTypes.GZIP, // compression to use for published messages, uses kafkajs enum
    groupId: null,                      // specify groupId, default: volante hub name will be used, so set this to your app name
    clientId: null,                     // specify clientId, default: volante hub name + hostname
    publishStar: false,                 // subscribe to all volante events and publish them to kafka
    publishStarTopic: 'volante',        // topic to use for publishing all volante events
  },
  stats: {
    totalPublishedMessages: 0,
    totalReceivedMessages: 0,
    numSubscriptions: 0,
    publishedVolanteEvents: 0,
  },
  data() {
    return {
      kafka: null,
      admin: null,
      producer: null,
      consumer: null,
    };
  },
  events: {
    'VolanteKafka.start'() {
      this.enabled = true;
      this.initialize();
    },
    'VolanteKafka.publish'(topic, msg, callback) {
      this.publish(...arguments);
    },
    'VolanteKafka.subscribe'(topic, callback) {
      this.subscribe(topic, callback);
    },
  },
  updated() {
    if (this.enabled) {
      this.initialize();
    }
    if (this.publishStar) {
      this.$warn('subcribing to all volante events, this could have performance implications');
      // tell the hub we want everything
      this.$hub.onAll(this.name, this.publishVolanteEvent);
    }
  },
  methods: {
    initialize() {
      if (!this.brokers || this.brokers.length === 0) {
        this.$warn('tried to initialize but there are no brokers configured');
        return;
      }
      // default the groupId and clientId if they werent specified
      this.groupId = this.groupId || this.$hub.name; // use the hub name for id
      // append the hostname to identify this instance
      this.clientId = this.clientId || `${this.$hub.name}-${require('os').hostname()}`;
      this.$log(`using groupId: ${this.groupId} and clientId/volanteId: ${this.clientId}`);

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
          // signal that we are ready
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
      // don't log in this function to avoid loops
      this.producer && this.producer.send({
        topic,
        compression: this.compression,
        messages: [{ value: msg }],
      }).then(() => {
        this.intervalPublishedMessages++;
        this.totalPublishedMessages++;
        callback && callback(null);
      }).catch((e) => {
        if (this.publishStar) {
          // log to bare console to avoid loops
          console.error(e);
        } else {
          this.$warn(e);
        }
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
    // Handler for publishing wildcard volante events
    //
    publishVolanteEvent(eventType, ...eventArgs) {
      if (this.$isReady) {
        let obj = {
          Timestamp: new Date().toISOString(),
          VolanteName: this.groupId,
          VolanteId: this.clientId,
          EventType: eventType,
          EventArgs: eventArgs,
        };
        this.publish(this.publishStarTopic, JSON.stringify(obj), () => {
          this.publishedVolanteEvents++;
        });
      }
    },
  },
};
