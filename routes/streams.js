const express = require("express");
const router = express.Router();
const { SDK, SchemaEncoder, zeroBytes32 } = require("@somnia-chain/streams");
const { createPublicClient, createWalletClient, http, toHex } = require("viem");
const { privateKeyToAccount } = require("viem/accounts");
const { waitForTransactionReceipt } = require("viem/actions");
const { dreamChain } = require("../dream-chain");
require("dotenv").config();

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
const encoder = new SchemaEncoder(playerSchema);

let schemaId;
let initPromise;

(async () => {
  try {
    schemaId = await sdk.streams.computeSchemaId(playerSchema);
    console.log("Schema ID:", schemaId);

    try {
      const txHash = await sdk.streams.registerDataSchemas(
        [
          {
            id: "player_score",
            schema: playerSchema,
            parentSchemaId: zeroBytes32,
          },
        ],
        true
      );

      if (txHash && typeof txHash === 'string' && txHash.startsWith('0x')) {
        await waitForTransactionReceipt(publicClient, { hash: txHash });
        console.log(`Schema registered with transaction: ${txHash}`);
      } else {
        console.log("Schema already registered — no action required.");
      }
    } catch (err) {
      if (err.message.includes("Nothing to register") || err.message.includes("SchemaAlreadyRegistered")) {
        console.log("Schema already exists on blockchain — ready to use.");
      } else {
        console.warn("Schema registration warning:", err.message);
      }
    }
  } catch (err) {
    console.error("Failed to initialize schema:", err.message);
  }
})();

router.get("/schema", async (req, res) => {
  try {
    if (!schemaId) {
      schemaId = await sdk.streams.computeSchemaId(playerSchema);
    }
    res.json({ schemaId });
  } catch (err) {
    res.status(500).json({ error: "Failed to compute schema ID", message: err.message });
  }
});

router.post("/publish", async (req, res) => {
  try {
    const { player, score } = req.body;

    if (!player || score == null) {
      return res
        .status(400)
        .json({ error: "Missing player or score" });
    }

    const data = encoder.encodeData([
      { name: "player", value: player, type: "address" },
      { name: "score", value: BigInt(score), type: "uint256" },
    ]);

    const dataStreams = [
      { id: toHex(`player-${Date.now()}`, { size: 32 }), schemaId, data },
    ];

    const tx = await sdk.streams.set(dataStreams);

    console.log(
      `Published: ${player} | Score ${score} | Tx ${tx}`
    );

    res.json({ success: true, txHash: tx });
  } catch (err) {
    console.error("Publish error:", err);
    res.status(500).json({ error: err.message });
  }
});

router.get("/data", async (req, res) => {
  try {
    if (!schemaId) {
      schemaId = await sdk.streams.computeSchemaId(playerSchema);
    }

    const publisher = process.env.PUBLISHER_WALLET;
    const allData = await sdk.streams.getAllPublisherDataForSchema(
      schemaId,
      publisher
    );

    if (!allData || !Array.isArray(allData) || allData.length === 0) {
      return res.json({
        totalPlayers: 0,
        leaderboard: []
      });
    }

    const formatted = allData.map((item) => {
      let player = "",
        score = "";
      for (const field of item) {
        const val = field.value?.value ?? field.value;
        if (field.name === "player") player = val;
        if (field.name === "score") score = Number(val);
      }
      return { player, score };
    });

    const bestScores = {};
    for (const entry of formatted) {
      if (!entry.player) continue;
      const current = bestScores[entry.player];
      if (!current || entry.score > current.score) {
        bestScores[entry.player] = entry;
      }
    }

    const leaderboard = Object.values(bestScores)
      .sort((a,b) => b.score - a.score)
      .map((e, index) => ({
        rank: index + 1,
        player: e.player,
        score: e.score.toString(),
      }));

    res.json({
      totalPlayers: leaderboard.length,
      leaderboard,
    });
  } catch (err) {
    console.error("Fetch error:", err.message);
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
