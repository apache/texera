import { Component, OnInit } from '@angular/core';
import { TourService } from 'ngx-tour-ngx-bootstrap';

@Component({
  selector: 'texera-product-tour',
  templateUrl: './product-tour.component.html',
  styleUrls: ['./product-tour.component.scss']
})
export class ProductTourComponent implements OnInit {

  constructor(public tourService: TourService) { 
  }

  ngOnInit() {
  }

}
