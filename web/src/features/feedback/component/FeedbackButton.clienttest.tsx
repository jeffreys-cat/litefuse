import { fireEvent, render, screen } from "@testing-library/react";
import { FeedbackButtonWrapper } from "./FeedbackButton";

const GITHUB_ISSUES_URL = "https://github.com/litefuse/litefuse/issues";

describe("FeedbackButtonWrapper", () => {
  it("links feature requests and bug reports to Litefuse GitHub Issues", () => {
    render(
      <FeedbackButtonWrapper>
        <button>Open feedback</button>
      </FeedbackButtonWrapper>,
    );

    fireEvent.click(screen.getByRole("button", { name: "Open feedback" }));

    const featureRequestLink = screen.getByRole("link", {
      name: "Submit Feature Request",
    });
    const bugReportLink = screen.getByRole("link", {
      name: "Report a Bug",
    });

    for (const link of [featureRequestLink, bugReportLink]) {
      expect(link.getAttribute("href")).toBe(GITHUB_ISSUES_URL);
      expect(link.getAttribute("target")).toBe("_blank");
      expect(link.getAttribute("rel")).toBe("noopener noreferrer");
    }
  });
});
