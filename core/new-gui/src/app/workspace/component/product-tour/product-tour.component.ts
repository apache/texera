import { Component, OnInit } from '@angular/core';
import { TourService } from 'ngx-tour-ngx-bootstrap';

@Component({
  selector: 'texera-product-tour',
  templateUrl: './product-tour.component.html',
  styleUrls: ['./product-tour.component.scss']
})
export class ProductTourComponent implements OnInit {

  public loaded = false;

  constructor(public tourService: TourService) { 

    this.tourService.anchorRegister$.subscribe((steps: string) => {
      console.log('tour configured with these steps:', steps);
    });

    this.tourService.initialize([{
      anchorId: 'texera-navigation-grid-container',
      content: '<center><h3>Welcome to Texera!</h3></center><br><p>Texera is a system to support cloud-based text analytics using declarative and GUI-based workflows.</p><br><center><img src="../../../assets/Tutor_Intro_Sample.png" height="400" width="800" alt="intro img"></center><br><br>',
      placement: 'bottom',
      title: 'Welcome'
    }])
  }

  ngOnInit() {
  }

}
