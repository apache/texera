import { Component, OnInit } from '@angular/core';
import { NgxBootstrapProductTourService } from 'ngx-bootstrap-product-tour';
@Component({
  selector: 'texera-product-tour',
  templateUrl: './product-tour.component.html',
  styleUrls: ['./product-tour.component.scss']
})
export class ProductTourComponent implements OnInit {

  public loaded = false;
  constructor(public tourService: NgxBootstrapProductTourService) { 
    this.loaded = false;
    this.tourService.events$.subscribe(console.log);
    this.tourService.initialize([{
      anchorId: 'start.tour',
      content: 'Welcome to the Ngx-Tour tour!',
      placement: 'right',
      title: 'Welcome',
      orphan: true,
      backdrop: true
    }]);
  }

  ngOnInit() {
  }

}
