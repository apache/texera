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
    expect(isImageUrl("data:image/png;base64,abc123")).toBeTrue();
    expect(isImageUrl("data:image/jpeg;base64,abc123")).toBeTrue();
    expect(isImageUrl("data:image/webp;base64,abc123")).toBeTrue();
  });

  it("should return true for common image file extensions", () => {
    expect(isImageUrl("https://example.com/photo.png")).toBeTrue();
    expect(isImageUrl("https://example.com/photo.jpg")).toBeTrue();
    expect(isImageUrl("https://example.com/photo.jpeg")).toBeTrue();
    expect(isImageUrl("https://example.com/photo.gif")).toBeTrue();
    expect(isImageUrl("https://example.com/photo.webp")).toBeTrue();
  });

  it("should be case-insensitive for extensions", () => {
    expect(isImageUrl("https://example.com/photo.PNG")).toBeTrue();
    expect(isImageUrl("https://example.com/photo.JPG")).toBeTrue();
  });

  it("should return true for URLs with query strings", () => {
    expect(isImageUrl("https://example.com/photo.png?v=1")).toBeTrue();
  });

  it("should return false for audio and video URLs", () => {
    expect(isImageUrl("data:audio/mp3;base64,abc")).toBeFalse();
    expect(isImageUrl("data:video/mp4;base64,abc")).toBeFalse();
    expect(isImageUrl("https://example.com/clip.mp4")).toBeFalse();
  });

  it("should return false for plain text strings", () => {
    expect(isImageUrl("hello world")).toBeFalse();
    expect(isImageUrl("")).toBeFalse();
  });
});

describe("isAudioUrl", () => {
  it("should return true for data:audio/ data URLs", () => {
    expect(isAudioUrl("data:audio/mp3;base64,abc123")).toBeTrue();
    expect(isAudioUrl("data:audio/wav;base64,abc123")).toBeTrue();
  });

  it("should return true for common audio file extensions", () => {
    expect(isAudioUrl("https://example.com/clip.mp3")).toBeTrue();
    expect(isAudioUrl("https://example.com/clip.wav")).toBeTrue();
    expect(isAudioUrl("https://example.com/clip.ogg")).toBeTrue();
    expect(isAudioUrl("https://example.com/clip.m4a")).toBeTrue();
    expect(isAudioUrl("https://example.com/clip.flac")).toBeTrue();
  });

  it("should be case-insensitive for extensions", () => {
    expect(isAudioUrl("https://example.com/clip.MP3")).toBeTrue();
    expect(isAudioUrl("https://example.com/clip.WAV")).toBeTrue();
  });

  it("should return true for URLs with query strings", () => {
    expect(isAudioUrl("https://example.com/clip.mp3?token=xyz")).toBeTrue();
  });

  it("should return false for image and video URLs", () => {
    expect(isAudioUrl("data:image/png;base64,abc")).toBeFalse();
    expect(isAudioUrl("data:video/mp4;base64,abc")).toBeFalse();
    expect(isAudioUrl("https://example.com/photo.png")).toBeFalse();
  });

  it("should return false for plain text strings", () => {
    expect(isAudioUrl("hello world")).toBeFalse();
    expect(isAudioUrl("")).toBeFalse();
  });
});

describe("isVideoUrl", () => {
  it("should return true for data:video/ data URLs", () => {
    expect(isVideoUrl("data:video/mp4;base64,abc123")).toBeTrue();
    expect(isVideoUrl("data:video/webm;base64,abc123")).toBeTrue();
  });

  it("should return true for common video file extensions", () => {
    expect(isVideoUrl("https://example.com/clip.mp4")).toBeTrue();
    expect(isVideoUrl("https://example.com/clip.webm")).toBeTrue();
    expect(isVideoUrl("https://example.com/clip.ogg")).toBeTrue();
  });

  it("should return true for fal.media CDN URLs", () => {
    expect(isVideoUrl("https://v3b.fal.media/files/abc123/output.mp4")).toBeTrue();
  });

  it("should be case-insensitive for extensions", () => {
    expect(isVideoUrl("https://example.com/clip.MP4")).toBeTrue();
    expect(isVideoUrl("https://example.com/clip.WEBM")).toBeTrue();
  });

  it("should return true for URLs with query strings", () => {
    expect(isVideoUrl("https://example.com/clip.mp4?t=5")).toBeTrue();
  });

  it("should return false for image and audio URLs", () => {
    expect(isVideoUrl("data:image/png;base64,abc")).toBeFalse();
    expect(isVideoUrl("data:audio/mp3;base64,abc")).toBeFalse();
    expect(isVideoUrl("https://example.com/photo.jpg")).toBeFalse();
  });

  it("should return false for plain text strings", () => {
    expect(isVideoUrl("hello world")).toBeFalse();
    expect(isVideoUrl("")).toBeFalse();
  });
});
