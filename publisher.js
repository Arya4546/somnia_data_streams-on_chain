const { SDK, SchemaEncoder, zeroBytes32 } = require("@somnia-chain/streams");
const { createPublicClient, http, createWalletClient, toHex } = require("viem");
const { privateKeyToAccount } = require("viem/accounts");
const { waitForTransactionReceipt } = require("viem/actions");
const { dreamChain } = require("./dream-chain");
require("dotenv").config();

async function main() {
  const publicClient = createPublicClient({
    chain: dreamChain,
    transport: http(),
  });
  const walletClient = createWalletClient({
    account: privateKeyToAccount(process.env.PRIVATE_KEY),
    chain: dreamChain,
    transport: http(),
  });

  const sdk = new SDK({ public: publicClient, wallet: walletClient });

  const playerSchema = `address player, uint256 score`;
  const schemaId = await sdk.streams.computeSchemaId(playerSchema);
  console.log("Schema ID:", schemaId);

  const ignoreAlreadyRegistered = true;

  try {
    const txHash = await sdk.streams.registerDataSchemas(
      [
        {
          id: "player_score",
          schema: playerSchema,
          parentSchemaId: zeroBytes32,
        },
      ],
      ignoreAlreadyRegistered
    );

    if (txHash) {
      await waitForTransactionReceipt(publicClient, { hash: txHash });
      console.log(`Schema registered or confirmed, Tx: ${txHash}`);
    } else {
      console.log("Schema already registered — no action required.");
    }
  } catch (err) {
    if (String(err).includes("SchemaAlreadyRegistered")) {
      console.log("Schema already registered. Continuing...");
    } else {
      throw err;
    }
  }

  const encoder = new SchemaEncoder(playerSchema);
  let count = 0;

  setInterval(async () => {
    count++;

    const randomScore = Math.floor(Math.random() * 1000);

    const data = encoder.encodeData([
      { name: "player", value: walletClient.account.address, type: "address" },
      { name: "score", value: BigInt(randomScore), type: "uint256" },
    ]);

    const dataStreams = [
      { id: toHex(`player-${count}`, { size: 32 }), schemaId, data },
    ];

    const tx = await sdk.streams.set(dataStreams);
    console.log(
      `Published: Player ${walletClient.account.address} | Score ${randomScore} (Tx: ${tx})`
    );
  }, 5000);
}

main();
