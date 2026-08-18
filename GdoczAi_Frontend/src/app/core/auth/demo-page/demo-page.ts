import { Component } from '@angular/core';
import { Playground } from '../../main/component/playground/playground';
import { AuthRoutingModule } from "../auth-routing-module";

@Component({
  selector: 'app-demo-page',
  imports: [Playground, AuthRoutingModule],
  templateUrl: './demo-page.html',
  styleUrl: './demo-page.scss',
})
export class DemoPage {

}
