const Pulsar = require("pulsar-client");

const SIGNALS = [`exit`, `SIGINT`, `SIGUSR1`, `SIGUSR2`, `SIGTERM`];

const SUBSCRIPTION_NAME = process.env.SUBSCRIPTION_NAME || "transit";

const PULSAR_URL = process.env.PULSAR_URL || "pulsar://127.0.0.1:6650";
const PULSAR_TOKEN = process.env.PULSAR_TOKEN;
const PULSAR_TOPIC = process.env.PULSAR_TOPIC;
const CLIENT_PARAMS = {
  serviceUrl: PULSAR_URL,
  operationTimeoutSeconds: 30,
  authentication: PULSAR_TOKEN ? new Pulsar.AuthenticationToken({ token: PULSAR_TOKEN }) : undefined,
};

const DEST_PULSAR_URL = process.env.DEST_PULSAR_URL || "pulsar://127.0.0.1:6650";
const DEST_PULSAR_TOKEN = process.env.DEST_PULSAR_TOKEN;
const DEST_PULSAR_TOPIC = process.env.DEST_PULSAR_TOPIC;
const DEST_CLIENT_PARAMS = {
  serviceUrl: DEST_PULSAR_URL,
  operationTimeoutSeconds: 30,
  authentication: DEST_PULSAR_TOKEN ? new Pulsar.AuthenticationToken({ token: DEST_PULSAR_TOKEN }) : undefined,
};

const client = new Pulsar.Client(CLIENT_PARAMS);
const destClient = new Pulsar.Client(DEST_CLIENT_PARAMS);

(async () => {
  let exiting;
  let msg;
  let consumer;
  let producer;

  const cleanupAndExit = async (eventType) => {
    if (exiting) {
      return;
    }
    console.log(eventType);
    exiting = true;

    if (consumer && consumer.isConnected()) {
      console.log("Closing consumer");
      await consumer.close().catch(console.error);
      console.log("Closed consumer");
    }

    if (producer && producer.isConnected()) {
      console.log("Closing producer");
      await producer.close().catch(console.error);
      console.log("Closed producer");
    }
    console.log("Closing client");
    await client?.close()?.catch(console.error);
    console.log("Closed client");
    console.log("Closing DEST client");
    await destClient?.close()?.catch(console.error);
    console.log("Closed DEST client");
    process.exit();
  };

  SIGNALS.forEach((eventType) => {
    process.on(eventType, cleanupAndExit.bind(null, eventType));
  });

  process
    .on("unhandledRejection", (reason, p) => {
      console.error(reason, "Unhandled Rejection at Promise", p);
    })
    .on("uncaughtException", (err) => {
      console.error(err, "Uncaught Exception thrown");
      cleanup("uncaughtException");
      process.exit(1);
    });

  consumer = await client
    .subscribe({
      topic: PULSAR_TOPIC,
      subscriptionInitialPosition: "Earliest",
      subscription: SUBSCRIPTION_NAME,
    })
    .catch(cleanupAndExit);

  producer = await destClient.createProducer({ topic: DEST_PULSAR_TOPIC });

  while ((msg = await consumer.receive())) {
    if (exiting) {
      break;
    }

    const payload = msg.getData().toString();

    const result = await producer.send({ data: Buffer.from(payload) });

    if (!result) cleanupAndExit;

    consumer.acknowledge(msg);
  }
})();
