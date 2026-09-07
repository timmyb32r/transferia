// @vitest-environment jsdom

import { cleanup } from "@testing-library/preact";
import { afterEach, expect, it } from "vitest";

import { ConnectionCheck } from "../src/delivery/ConnectionCheck";
import type { ConnectionCheckState } from "../src/delivery/useEndpointActions";
import { render } from "./support/render";

afterEach(cleanup);

const feedbackStates: ConnectionCheckState[] = [
  { state: "success", status: "verified", options: {}, message: "Connection verified, including access to the configured database and its tables." },
  { state: "success", status: "network_reachable", options: {}, message: "Network connection is available, but authentication and access to the configured tables were not checked." },
  { state: "error", options: {}, message: "Authentication failed. Check the username, password and database access permissions before trying again." },
  { state: "error", options: {}, message: `Connection failed.\n${"A_long_diagnostic_without_spaces_".repeat(20)}\nLast diagnostic line.` },
];

it.each(feedbackStates)("preserves the full $state diagnostic in accessible text and its hover hint", check => {
  const view = render(<ConnectionCheck check={check} onCheck={() => undefined} />);
  const result = view.getByRole(check.state === "error" ? "alert" : "status");
  expect(result.textContent).toBe("message" in check ? check.message : undefined);
  expect(result.getAttribute("title")).toBe(result.textContent);
  expect(result.getAttribute("aria-atomic")).toBe("true");
  expect(result.tabIndex).toBe(0);
});

it("retains the same feedback slot and surrounding controls across connection states", () => {
  const onCheck = () => undefined;
  const view = render(<ConnectionCheck check={{ state: "idle", options: {} }} onCheck={onCheck} />);
  const button = view.getByRole("button", { name: "Check connection" });
  const result = view.getByRole("status");
  const slots = Array.from(result.parentElement!.children);
  expect(result.hasAttribute("title")).toBe(false);

  for (const check of [{ state: "checking", options: {} } as const, ...feedbackStates, { state: "idle", options: {} } as const]) {
    view.rerender(<ConnectionCheck check={check} onCheck={onCheck} />);
    expect(view.getByRole("button", { name: "Check connection" })).toBe(button);
    expect(view.getByRole(check.state === "error" ? "alert" : "status")).toBe(result);
    expect(Array.from(result.parentElement!.children)).toEqual(slots);
    expect(button.getAttribute("aria-busy")).toBe(String(check.state === "checking"));
  }
  expect(result.hasAttribute("title")).toBe(false);
});
