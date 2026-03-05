import { PutObjectCommand } from "@aws-sdk/client-s3";
import { getS3Client } from "./s3Client.js";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} ausente no .env`);
  return v;
}

export async function putObjectBuffer({ Key, Body, ContentType }) {
  const Bucket = mustEnv("S3_BUCKET");
  const s3 = getS3Client();

  await s3.send(
    new PutObjectCommand({
      Bucket,
      Key,
      Body,
      ContentType: ContentType || "application/octet-stream",
    })
  );
}