import { render, screen } from "@testing-library/react";
import { type ReactNode } from "react";
import { Drawer } from "./drawer";

type DrawerRootProps = {
  children?: ReactNode;
  dismissible?: boolean;
};

jest.mock("react-responsive", () => ({
  useMediaQuery: () => false,
}));

jest.mock("vaul", () => ({
  Drawer: {
    Root: ({ children, dismissible }: DrawerRootProps) => (
      <div data-testid="drawer-root" data-dismissible={String(dismissible)}>
        {children}
      </div>
    ),
    Trigger: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Portal: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Overlay: () => null,
    Content: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Close: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Title: ({ children }: { children?: ReactNode }) => <>{children}</>,
    Description: ({ children }: { children?: ReactNode }) => <>{children}</>,
  },
}));

describe("Drawer", () => {
  it("is dismissible by default", () => {
    render(<Drawer>Drawer content</Drawer>);

    expect(
      screen.getByTestId("drawer-root").getAttribute("data-dismissible"),
    ).toBe("true");
  });
});
