/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { isAudioUrl, isImageUrl, isVideoUrl } from "./media-type.util";

describe("isImageUrl", () => {
  it("should return true for data:image/ data URLs", () => {
    expect(isImageUrl("data:image/png;base64,abc123")).toBe(true);
    expect(isImageUrl("data:image/jpeg;base64,abc123")).toBe(true);
    expect(isImageUrl("data:image/webp;base64,abc123")).toBe(true);
  });

  it("should return true for common image file extensions", () => {
    expect(isImageUrl("https://example.com/photo.png")).toBe(true);
    expect(isImageUrl("https://example.com/photo.jpg")).toBe(true);
    expect(isImageUrl("https://example.com/photo.jpeg")).toBe(true);
    expect(isImageUrl("https://example.com/photo.gif")).toBe(true);
    expect(isImageUrl("https://example.com/photo.webp")).toBe(true);
  });

  it("should be case-insensitive for extensions", () => {
    expect(isImageUrl("https://example.com/photo.PNG")).toBe(true);
    expect(isImageUrl("https://example.com/photo.JPG")).toBe(true);
  });

  it("should return true for URLs with query strings", () => {
    expect(isImageUrl("https://example.com/photo.png?v=1")).toBe(true);
  });

  it("should return false for audio and video URLs", () => {
    expect(isImageUrl("data:audio/mp3;base64,abc")).toBe(false);
    expect(isImageUrl("data:video/mp4;base64,abc")).toBe(false);
    expect(isImageUrl("https://example.com/clip.mp4")).toBe(false);
  });

  it("should return false for plain text strings", () => {
    expect(isImageUrl("hello world")).toBe(false);
    expect(isImageUrl("")).toBe(false);
  });
});

describe("isAudioUrl", () => {
  it("should return true for data:audio/ data URLs", () => {
    expect(isAudioUrl("data:audio/mp3;base64,abc123")).toBe(true);
    expect(isAudioUrl("data:audio/wav;base64,abc123")).toBe(true);
  });

  it("should return true for common audio file extensions", () => {
    expect(isAudioUrl("https://example.com/clip.mp3")).toBe(true);
    expect(isAudioUrl("https://example.com/clip.wav")).toBe(true);
    expect(isAudioUrl("https://example.com/clip.ogg")).toBe(true);
    expect(isAudioUrl("https://example.com/clip.m4a")).toBe(true);
    expect(isAudioUrl("https://example.com/clip.flac")).toBe(true);
  });

  it("should be case-insensitive for extensions", () => {
    expect(isAudioUrl("https://example.com/clip.MP3")).toBe(true);
    expect(isAudioUrl("https://example.com/clip.WAV")).toBe(true);
  });

  it("should return true for URLs with query strings", () => {
    expect(isAudioUrl("https://example.com/clip.mp3?token=xyz")).toBe(true);
  });

  it("should return false for image and video URLs", () => {
    expect(isAudioUrl("data:image/png;base64,abc")).toBe(false);
    expect(isAudioUrl("data:video/mp4;base64,abc")).toBe(false);
    expect(isAudioUrl("https://example.com/photo.png")).toBe(false);
  });

  it("should return false for plain text strings", () => {
    expect(isAudioUrl("hello world")).toBe(false);
    expect(isAudioUrl("")).toBe(false);
  });
});

describe("isVideoUrl", () => {
  it("should return true for data:video/ data URLs", () => {
    expect(isVideoUrl("data:video/mp4;base64,abc123")).toBe(true);
    expect(isVideoUrl("data:video/webm;base64,abc123")).toBe(true);
  });

  it("should return true for common video file extensions", () => {
    expect(isVideoUrl("https://example.com/clip.mp4")).toBe(true);
    expect(isVideoUrl("https://example.com/clip.webm")).toBe(true);
    expect(isVideoUrl("https://example.com/clip.ogv")).toBe(true);
  });

  it("should return true for fal.media CDN URLs", () => {
    expect(isVideoUrl("https://v3b.fal.media/files/abc123/output.mp4")).toBe(true);
  });

  it("should be case-insensitive for extensions", () => {
    expect(isVideoUrl("https://example.com/clip.MP4")).toBe(true);
    expect(isVideoUrl("https://example.com/clip.WEBM")).toBe(true);
  });

  it("should return true for URLs with query strings", () => {
    expect(isVideoUrl("https://example.com/clip.mp4?t=5")).toBe(true);
  });

  it("should return false for image and audio URLs", () => {
    expect(isVideoUrl("data:image/png;base64,abc")).toBe(false);
    expect(isVideoUrl("data:audio/mp3;base64,abc")).toBe(false);
    expect(isVideoUrl("https://example.com/photo.jpg")).toBe(false);
  });

  it("should return false for plain text strings", () => {
    expect(isVideoUrl("hello world")).toBe(false);
    expect(isVideoUrl("")).toBe(false);
  });

  it("should return false for non-string types", () => {
    expect(isVideoUrl(null as unknown as string)).toBe(false);
    expect(isVideoUrl(undefined as unknown as string)).toBe(false);
    expect(isVideoUrl(42 as unknown as string)).toBe(false);
  });
});

describe("non-string type guard (shared)", () => {
  it("isAudioUrl should return false for non-string types", () => {
    expect(isAudioUrl(null as unknown as string)).toBe(false);
    expect(isAudioUrl(undefined as unknown as string)).toBe(false);
    expect(isAudioUrl(true as unknown as string)).toBe(false);
  });

  it("isImageUrl should return false for non-string types", () => {
    expect(isImageUrl(null as unknown as string)).toBe(false);
    expect(isImageUrl(undefined as unknown as string)).toBe(false);
    expect(isImageUrl([] as unknown as string)).toBe(false);
  });
});
