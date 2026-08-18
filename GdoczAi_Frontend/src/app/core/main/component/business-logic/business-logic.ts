import { Component, inject } from '@angular/core';
import { Router, RouterModule } from '@angular/router';

@Component({
  selector: 'app-business-logic',
  imports: [RouterModule],
  templateUrl: './business-logic.html',
  styleUrl: './business-logic.scss',
})
export class BusinessLogic {
  readonly router = inject(Router)

  hideButton: boolean = false

  ngOnInit() {
    this.hideButton = this.router.url.includes('create');
  }

}
