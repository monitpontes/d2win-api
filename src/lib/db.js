
import mongoose from "mongoose";

export const connectMongo = async (uri) => {
  mongoose.set("strictQuery", true);
  await mongoose.connect(uri, { dbName: undefined });
  console.log("Mongo connected");
};

export const db = () => mongoose.connection;
