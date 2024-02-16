import { Pipe, PipeTransform } from "@angular/core";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
@Pipe({
  name: "highlightSearchTerms",
})
export class HighlightSearchTermsPipe implements PipeTransform {
  constructor(private sanitizer: DomSanitizer) {}

  transform(value: string | undefined, terms: string[]): SafeHtml {
    if (!terms || !terms.length || !value) {
      // Return the original value if there's nothing to highlight or if the value is undefined
      return this.sanitizer.bypassSecurityTrustHtml(value || "");
    }

    // Escape the terms to be used in a RegExp
    const regex = new RegExp(`(${terms.join("|")})`, "gi");

    const highlightedString = value.replace(regex, "<span class=\"highlight-search-terms\">$1</span>");
    // Use the sanitizer to avoid security risks
    return this.sanitizer.bypassSecurityTrustHtml(highlightedString);
  }
}
