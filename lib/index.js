const amqplib = require('amqplib');

const RABBIT_URL = process.env.RABBIT_URL || 'amqp://127.0.0.1';

class EventsProxy {
    constructor() {
        this.channel = null;

        this.init = this.init.bind(this);
        this.subscribe = this.subscribe.bind(this);
        this.publish = this.publish.bind(this);
        this.subscribeError = this.subscribeError.bind(this);
    }

    async init() {
        const connection = await amqplib.connect(RABBIT_URL);

        this.channel = await connection.createChannel();
    }

    subscribe(event, callback) {
        this.channel.assertQueue(event, { durable: false });

        this.channel.consume(event, (msg) => {
            const msgObject = JSON.parse(msg.content.toString());

            const promise = callback(msgObject);

            if (promise) {
                promise
                    .then(() => {
                        this.channel.ack(msg);
                    })
                    .catch(e => {
                        console.error(e);
                        this.publish(`${event}_error`, msgObject);
                        this.channel.ack(msg);
                    });
            }
        });
    }

    subscribeError(event, callback) {
        this.subscribe(`${event}_error`, callback);
    }


    publish(event, data) {
        this.channel.assertQueue(event, { durable: false });

        this.channel.sendToQueue(event, Buffer.from(JSON.stringify(data)));
    }
}

module.exports = new EventsProxy();
