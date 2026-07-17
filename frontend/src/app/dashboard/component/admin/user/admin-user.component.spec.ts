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

import { ComponentFixture, inject, TestBed } from "@angular/core/testing";
import { AdminUserComponent } from "./admin-user.component";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { AdminUserService } from "../../../service/admin/user/admin-user.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { FormsModule } from "@angular/forms";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("AdminUserComponent", () => {
  let component: AdminUserComponent;
  let fixture: ComponentFixture<AdminUserComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      providers: [{ provide: UserService, useClass: StubUserService }, AdminUserService, ...commonTestProviders],
      imports: [AdminUserComponent, FormsModule, HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminUserComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));

  it("should search email case-insensitively", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
      {
        name: "Bob",
        email: "bob@example.com",
        comment: "Approved",
      } as any,
    ];

    component.emailSearchValue = "ALICE@EXAMPLE.COM";
    component.searchByEmail();

    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].email).toBe("alice@example.com");
  });

  it("should search comment case-insensitively", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
      {
        name: "Bob",
        email: "bob@example.com",
        comment: "Approved",
      } as any,
    ];

    component.commentSearchValue = "NEEDS REVIEW";
    component.searchByComment();

    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].comment).toBe("Needs review");
  });

  it("should trim active email search value without lowercasing the stored input", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
    ];

    component.emailSearchValue = "  ALICE@EXAMPLE.COM  ";
    component.searchByEmail();

    expect(component.emailSearchValue).toBe("ALICE@EXAMPLE.COM");
    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].email).toBe("alice@example.com");
  });

  it("should trim active comment search value without lowercasing the stored input", () => {
    component.userList = [
      {
        name: "Alice",
        email: "alice@example.com",
        comment: "Needs review",
      } as any,
    ];

    component.commentSearchValue = "  Needs review  ";
    component.searchByComment();

    expect(component.commentSearchValue).toBe("Needs review");
    expect(component.listOfDisplayUser.length).toBe(1);
    expect(component.listOfDisplayUser[0].comment).toBe("Needs review");
  });

  it("should clear inactive search values when searching by name", () => {
    component.emailSearchValue = "alice@example.com";
    component.commentSearchValue = "Needs review";

    component.nameSearchValue = "Alice";
    component.searchByName();

    expect(component.emailSearchValue).toBe("");
    expect(component.commentSearchValue).toBe("");
  });

  it("should clear inactive search values when searching by email", () => {
    component.nameSearchValue = "Alice";
    component.commentSearchValue = "Needs review";

    component.emailSearchValue = "bob@example.com";
    component.searchByEmail();

    expect(component.nameSearchValue).toBe("");
    expect(component.commentSearchValue).toBe("");
  });

  it("should clear inactive search values when searching by comment", () => {
    component.nameSearchValue = "Alice";
    component.emailSearchValue = "alice@example.com";

    component.commentSearchValue = "Approved";
    component.searchByComment();

    expect(component.nameSearchValue).toBe("");
    expect(component.emailSearchValue).toBe("");
  });
});
