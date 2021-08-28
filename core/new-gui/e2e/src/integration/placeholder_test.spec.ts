// <reference types="cypress" />
describe("Cypress works!", () => {
  it("Boolean logic", () => {
    expect(true).to.equal(true);
    expect(false).to.equal(false);
    expect(false).to.not.equal(true);
  });
});

describe("Cypress Workflow", () => {
  it("Set up application state", () => {
    cy.visit("localhost:8080");
  });
  it("watch mode", () => {
    expect(true).to.equal(true);
  });
});
