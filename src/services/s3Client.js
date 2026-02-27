// src/services/s3Client.js
import { S3Client } from "@aws-sdk/client-s3";

export function getS3Client() {
  const region = process.env.AWS_REGION;
  if (!region) throw new Error("AWS_REGION ausente no .env");
  return new S3Client({ region });
}