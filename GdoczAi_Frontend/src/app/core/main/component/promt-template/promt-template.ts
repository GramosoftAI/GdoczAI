import { Component, inject } from '@angular/core';
import { Router, RouterModule } from '@angular/router';
import { PromptTemplateList } from "./prompt-template-list/prompt-template-list";

@Component({
  selector: 'app-promt-template',
  imports: [RouterModule],
  templateUrl: './promt-template.html',
  styleUrl: './promt-template.scss',
})
export class PromtTemplate {
  public router = inject(Router)

}
