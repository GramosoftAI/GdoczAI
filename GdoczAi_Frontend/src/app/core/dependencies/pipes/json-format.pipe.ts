// json-format.pipe.ts
import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'jsonFormat'
})
export class JsonFormatPipe implements PipeTransform {
  transform(value: any): string {
    if (!value) return '';

    try {
      // If it's already a string, parse it first
      const jsonObj = typeof value === 'string' ? JSON.parse(value) : value;
      return JSON.stringify(jsonObj, null, 2);
    } catch (error) {
      // If parsing fails, return the original value
      return String(value);
    }
  }
}
