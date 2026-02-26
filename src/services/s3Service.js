import { PutObjectCommand } from "@aws-sdk/client-s3";
import s3 from "../lib/s3.js";

export async function uploadTestFile() {
  const command = new PutObjectCommand({
    Bucket: process.env.S3_BUCKET,
    Key: "test/teste.txt",
    Body: "Teste D2WIN S3 funcionando",
    ContentType: "text/plain",
  });

  await s3.send(command);
}