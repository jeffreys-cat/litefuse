import { render, screen } from "@testing-library/react";
import { ErrorNotification } from "./ErrorNotification";
import { SupportDrawerProvider } from "../support-chat/SupportDrawerProvider";

const GITHUB_ISSUES_URL = "https://github.com/litefuse/litefuse/issues";

const defaultProps = {
  error: "Unexpected Error",
  description: "Something went wrong",
  dismissToast: jest.fn(),
  toast: "toast-id",
};

describe("ErrorNotification", () => {
  it("links errors to Litefuse GitHub Issues", () => {
    render(
      <SupportDrawerProvider>
        <ErrorNotification {...defaultProps} type="ERROR" />
      </SupportDrawerProvider>,
    );

    const reportIssueLink = screen.getByRole("link", {
      name: "Report issue to Litefuse team",
    });

    expect(reportIssueLink.getAttribute("href")).toBe(GITHUB_ISSUES_URL);
    expect(reportIssueLink.getAttribute("target")).toBe("_blank");
    expect(reportIssueLink.getAttribute("rel")).toBe("noopener noreferrer");
  });

  it("does not show the report issue link for warnings", () => {
    render(
      <SupportDrawerProvider>
        <ErrorNotification {...defaultProps} type="WARNING" />
      </SupportDrawerProvider>,
    );

    expect(
      screen.queryByRole("link", {
        name: "Report issue to Litefuse team",
      }),
    ).toBeNull();
  });
});
