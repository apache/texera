import { Component, OnInit, Input } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

/**
 * ResultDownloadComponent is the popup when the download finish
 */
@Component({
  selector: 'texera-result-download',
  templateUrl: './result-download.component.html',
  styleUrls: ['./result-download.component.scss']
})
export class ResultDownloadComponent implements OnInit {
  @Input() message: string | undefined;
  @Input() link: string | undefined;
  constructor(public activeModal: NgbActiveModal) { }

  ngOnInit(): void {
  }


  /**
   * open the link in the new tab
   * @param href the link
   */
  private openInNewTab() {
    Object.assign(document.createElement('a'), {
      target: '_blank',
      href: this.link,
    }).click();
  }

}
