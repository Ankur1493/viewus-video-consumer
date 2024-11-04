import { DeleteMessageCommand, ReceiveMessageCommand, SQSClient } from "@aws-sdk/client-sqs";
import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3"
import ffmpeg from "fluent-ffmpeg"
import fs from "fs/promises";
import { createReadStream } from "fs";
import path from "path";
import dotenv from "dotenv"

dotenv.config();

const accessKeyId = process.env.ACCESS_KEY
const secretAccessKey = process.env.SECRET_ACCESS_KEY

const client = new SQSClient({
  credentials: {
    accessKeyId: accessKeyId,
    secretAccessKey: secretAccessKey
  },
  region: "ap-south-1"
})

const s3Client = new S3Client({
  credentials: {
    accessKeyId: accessKeyId,
    secretAccessKey: secretAccessKey
  },
  region: "ap-south-1"
})

let isProcessing = false;

async function init() {
  const command = new ReceiveMessageCommand({
    QueueUrl: "https://sqs.ap-south-1.amazonaws.com/339712935294/viewusVideoFetchingQueue",
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20
  })

  while (true) {
    const { Messages } = await client.send(command)
    if (!Messages) {
      console.log("no message in queue")
      continue;
    }

    try {
      for (const message of Messages) {
        if (isProcessing) {
          console.log("Video is currently being processed, skipping...");
          continue;
        }
        isProcessing = true;
        const { MessageId, Body } = message
        console.log(`message received ${MessageId} - ${Body}`)

        if (!Body) continue;
        //validate and parse the event
        const event = JSON.parse(Body)

        //ignores the test event
        if ("Service" in event && "Event" in event) {
          if (event.Event === "s3:TestEvent") continue;
        }

        //process the video here only
        for (const record of event.Records) {
          const { s3 } = record
          const {
            bucket,
            object: { key },
          } = s3

          await processVideo({ bucketName: bucket.name, key })
          //delete the event from queue
          const deleteCommand = new DeleteMessageCommand({
            QueueUrl: "https://sqs.ap-south-1.amazonaws.com/339712935294/viewusVideoFetchingQueue",
            ReceiptHandle: message.ReceiptHandle
          });
          const deleteS3videoCommand = new DeleteObjectCommand({
            Bucket: "temp-videos-viewus.in",
            Key: key,
          });
          await client.send(deleteCommand)
          console.log(`${key} message deleted from queue`)

          await s3Client.send(deleteS3videoCommand)
          console.log(`${key} video deleted from temps3`)
        }

        //after processing video
        isProcessing = false; // Reset flag
      }
    } catch (err) {
      console.log(err)
    }
  }
}

init();

async function processVideo({ key }) {
  console.log({ key })
  const command = new GetObjectCommand({
    Bucket: "temp-videos-viewus.in",
    Key: key,
  });

  const result = await s3Client.send(command);
  if (!result.Body) return { err: "Failed to fetch the video from S3" };

  const originalFilePath = `original-video.mp4`;

  // Save the S3 object body to a file
  await fs.writeFile(originalFilePath, result.Body);

  const originalVideoPath = path.resolve(originalFilePath);
  const outputDir = path.resolve("output");
  const playlistName = "playlist.m3u8";

  await fs.mkdir(outputDir, { recursive: true });

  return new Promise((resolve, reject) => {
    ffmpeg(originalVideoPath)
      .outputOptions([
        "-profile:v baseline", // H.264 video codec
        "-level 3.0",
        "-start_number 0",
        "-hls_time 10", // Each segment duration in seconds
        "-hls_list_size 0", // Make the playlist contain all segments
        "-f hls", // Output format
      ])
      .size("1280x720") // Resize to 720p
      .output(path.join(outputDir, playlistName))
      .on("end", async () => {
        console.log("Video processing complete, uploading to S3...");

        const files = await fs.readdir(outputDir);
        for (const file of files) {
          const filePath = path.join(outputDir, file);
          const fileStream = createReadStream(filePath);

          const uploadCommand = new PutObjectCommand({
            Bucket: "viewus.in-ankur-images-bucket",
            Key: `${key}/${file}`,
            Body: fileStream,
          });

          await s3Client.send(uploadCommand);
        }
        console.log("uploaded to s3")

        await fs.rm(outputDir, { recursive: true, force: true });
        console.log("files removed from local")
        resolve();
      })
      .on("error", (err) => {
        console.error("Error during video processing:", err);
        reject(err);
      })
      .run();
  });
}

