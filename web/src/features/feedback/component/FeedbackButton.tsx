import { Button } from "@/src/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogDescription,
  DialogPortal,
  DialogBody,
} from "@/src/components/ui/dialog";
import { LITEFUSE_GITHUB_ISSUES_URL } from "@/src/utils/constants";
import { useState } from "react";
import { Bug, Sparkles } from "lucide-react";

interface FeedbackDialogProps {
  className?: string;
  children: React.ReactNode;
  title?: string;
  description?: string;
}

export function FeedbackButtonWrapper({
  className,
  children,
  description = "What do you think about Litefuse? What can be improved? Please share it with the community on GitHub to shape the future of Litefuse.",
  title = "Provide Feedback",
}: FeedbackDialogProps) {
  const [open, setOpen] = useState(false);

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger
        className={className}
        asChild
        onClick={(e) => e.stopPropagation()}
      >
        {children}
      </DialogTrigger>
      <DialogPortal>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{title}</DialogTitle>
            <DialogDescription>{description}</DialogDescription>
          </DialogHeader>
          <DialogBody>
            <div className="flex flex-row flex-wrap items-center justify-center gap-3 sm:justify-start">
              <Button variant="secondary" asChild>
                <a
                  href={LITEFUSE_GITHUB_ISSUES_URL}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Sparkles className="mr-2 h-4 w-4" /> Submit Feature Request
                </a>
              </Button>
              <Button variant="secondary" asChild>
                <a
                  href={LITEFUSE_GITHUB_ISSUES_URL}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Bug className="mr-2 h-4 w-4" /> Report a Bug
                </a>
              </Button>
            </div>
          </DialogBody>
        </DialogContent>
      </DialogPortal>
    </Dialog>
  );
}
