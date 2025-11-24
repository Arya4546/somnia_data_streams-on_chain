const { SDK, SchemaEncoder } = require("@somnia-chain/streams");
const { createPublicClient, http } = require("viem");
const { dreamChain } = require("./dream-chain");
require("dotenv").config();

async function main() {
  const publisherWallet = process.env.PUBLISHER_WALLET;
  const publicClient = createPublicClient({
    chain: dreamChain,
    transport: http(),
  });
  const sdk = new SDK({ public: publicClient });

  const playerSchema = `address player, uint256 score`;
  const schemaId = await sdk.streams.computeSchemaId(playerSchema);

  const schemaEncoder = new SchemaEncoder(playerSchema);
  const seen = new Set();

  setInterval(async () => {
    const allData = await sdk.streams.getAllPublisherDataForSchema(
      schemaId,
      publisherWallet
    );

    for (const dataItem of allData) {
      let player = "",
        score = "";

      for (const field of dataItem) {
        const val = field.value?.value ?? field.value;
        if (field.name === "player") player = val;
        if (field.name === "score") score = val.toString();
      }

      const id = `${player}-${score}`;
      if (!seen.has(id)) {
        seen.add(id);
        console.log(
          `Player: ${player}\n   Score: ${score}\n`
        );
      }
    }
  }, 3000);
}

main();
