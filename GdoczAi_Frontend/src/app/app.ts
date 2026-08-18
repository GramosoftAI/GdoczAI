import { Component, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { Spinner } from "./core/dependencies/components/spinner/spinner";
import { Toastr } from './core/dependencies/components/toastr/toastr';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, Spinner, Toastr],
  templateUrl: './app.html',
  styleUrl: './app.scss'
})
export class App {
  protected readonly title = signal('datalab');
}
