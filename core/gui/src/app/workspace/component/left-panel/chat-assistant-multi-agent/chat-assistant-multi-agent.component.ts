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

import { Component, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ChatAssistantWebsocketService } from "../../../service/chat-assistant-websocket/chat-assistant-websocket.service";

@UntilDestroy()
@Component({
  selector: "texera-chat-assistant-non-streamed",
  templateUrl: "./chat-assistant-multi-agent.component.html",
  styleUrls: ["./chat-assistant-multi-agent.component.scss"],
})
export class ChatAssistantMultiAgentComponent implements OnInit {
  messages: { sender: string; content: string }[] = [];
  newMessage: string = "";
  isWaitingForResponse = false;
  // Keeps track of the last AI message so we can append text deltas to it.
  private aiMessageIndex?: number;

  constructor(private chatAssistantWebsocketService: ChatAssistantWebsocketService) {}

  ngOnInit(): void {
    // Start WebSocket connection and subscribe to streaming response events.
    this.connectToPythonServer();
    this.subscribeToStreamResponses();
  }

  sendMessage() {
    if (!this.newMessage.trim()) {
      return;
    }

    const userMsg = this.newMessage.trim();
    this.messages.push({ sender: "User", content: userMsg });
    this.newMessage = "";
    this.isWaitingForResponse = true;

    // Send the user message to the Python server.
    this.chatAssistantWebsocketService.send("ChatUserMessageRequest", { message: userMsg });

    // Add an empty AI message entry; this will be updated as responses stream in.
    this.aiMessageIndex = this.messages.push({ sender: "AI", content: "" }) - 1;
  }

  private connectToPythonServer(): void {
    this.chatAssistantWebsocketService.closeWebsocket();
    this.chatAssistantWebsocketService.openWebsocket();
  }

  private subscribeToStreamResponses(): void {
    // Subscribe to each delta of the AI response.
    this.chatAssistantWebsocketService
      .subscribeToEvent("ChatStreamResponseEvent")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: (event: { type: "ChatStreamResponseEvent"; delta: string }) => {
          if (this.aiMessageIndex !== undefined) {
            this.messages[this.aiMessageIndex].content += event.delta;
          }
        },
        error: (err: unknown) => {
          console.error("Error in chat stream:", err);
          this.isWaitingForResponse = false;
        },
      });

    // Subscribe to the stream completion event.
    this.chatAssistantWebsocketService
      .subscribeToEvent("ChatStreamResponseComplete")
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          // Mark the stream as complete.
          this.isWaitingForResponse = false;
          this.aiMessageIndex = undefined;
          console.log("Chat stream completed.");
        },
        error: (err: unknown) => {
          console.error("Error in stream completion event:", err);
        },
      });
  }
}
