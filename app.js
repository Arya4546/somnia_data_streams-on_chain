const express = require("express");
const dotenv = require("dotenv");
const cors = require("cors");
const streamRoutes = require("./routes/streams");

dotenv.config();

const app = express();

app.use(cors({
  origin: "*",
  methods: ["GET", "POST"],
  allowedHeaders: ["Content-Type", "Authorization"]
}));

app.use(express.json());

app.use("/api", streamRoutes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () =>
  console.log(`🚀 Express backend running on port ${PORT}`)
);
