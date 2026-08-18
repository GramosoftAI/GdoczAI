import { AbstractControl, FormGroup, ValidationErrors, ValidatorFn } from '@angular/forms';

export function numbersOnlyValidator(): ValidatorFn {
  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;
    if (value && isNaN(value)) {
      return { numbersOnly: true };
    }
    return null;
  };
}

export function numbersAndDotOnlyValidator(): ValidatorFn {
  const pattern = /^[0-9]*\.?[0-9]+$/; // Allows numbers with an optional single dot for floating-point numbers

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;
    if (value && !pattern.test(value)) {
      return { numbersAndDotOnly: true };
    }
    return null;
  };
}

export function emailOnlyValidator(): ValidatorFn {
  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;
    if (value && isNaN(value)) {
      return { phoneEmailRegex: true };
    }
    return null;
  };
}

export function landlineValidator(): ValidatorFn {
  const pattern = /^(\d{3,5}([- ]*)\d{6})$/ // Allows letters, numbers, -, and /

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'landlineValidator': true };
    }

    return null;
  };
}

// form-validation.directive.ts
export function noLeadingSpaceValidator() {
  return (control: any) => {
    if (!control.value) {
      return null;
    }

    // Handle both strings and other types
    const value = typeof control.value === 'string' ? control.value : String(control.value);

    if (value.trim() !== value) {
      return { leadingSpace: true };
    }
    return null;
  };
}

export function notOnlyWhitespace() {
  return (control: any) => {
    if (!control.value) {
      return null;
    }

    // Handle both strings and other types
    const value = typeof control.value === 'string' ? control.value : String(control.value);

    if (value.trim().length === 0) {
      return { onlyWhitespace: true };
    }
    return null;
  };
}


export function noNumbersOrSpecialChars(): ValidatorFn {
  const pattern = /^[A-Za-z\s]+$/; // Only allows letters and spaces

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'noNumbersOrSpecialChars': true };
    }

    return null;
  };
}

export function addressValidator(): ValidatorFn {
  const pattern = /^[A-Za-z0-9\-/]*$/; // Allows letters, numbers, -, and /

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'addressValidator': true };
    }

    return null;
  };
}

export function noSpecialCharacters(): ValidatorFn {
  const pattern = /^[A-Za-z0-9\s]*$/; // Allows letters, numbers, and spaces

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'noSpecialCharacters': true };
    }

    return null;
  };
}

export function allowEmojisAndSpecialCharacters(): ValidatorFn {
  return (control: AbstractControl): ValidationErrors | null => {
    const value = control.value;
    const isValid = !/your-regex-for-emojis-and-special-chars/.test(value);
    return isValid ? null : { invalidInput: true };
  };
};

export function passwordValidator(): ValidatorFn {
  const pattern = /^(?=.*\d)(?=.*[!@#$%^&*])(?=.*[a-z])(?=.*[A-Z]).{8,}$/;
  // Validators.pattern("(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[#$@$!%*?&])[A-Za-z\d#$@$!%*?&].{7,}")

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'passwordValidator': true };
    }

    return null;
  };
}

export function ConfirmPasswordValidators(controlName: string, matchingControlName: string) {
  return (formGroup: FormGroup) => {
    const control = formGroup.controls[controlName];
    const matchingControl = formGroup.controls[matchingControlName];

    if (control.value !== matchingControl.value) {
      matchingControl.setErrors({ mismatchedPasswords: true });
    } else {
      matchingControl.setErrors(null);
    }
  };
}


export function confirmPasswordValidator(control: AbstractControl): ValidationErrors | null {
  const password = control.get('password')?.value;
  const confirmPassword = control.get('confirmPassword')?.value;

  if (password !== confirmPassword) {
    return { 'passwordMismatch': true };
  }

  return null;
}



export function compareValuesValidator(compareKey: string): ValidatorFn {
  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;
    const compareValue = control.root.get(compareKey)?.value;

    if (compareValue !== null && value !== null && compareValue >= value) {
      return { invalidValue: true };
    }

    return null;
  };
}

export function gstNumberValidator(): ValidatorFn {
  const pattern = /^(\d{2}[A-Z]{5}\d{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1})$/;
  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'invalidGstNumber': true };
    }

    return null;
  };
}

export function textOnlyValidator(): ValidatorFn {
  const pattern = /^[A-Za-z\s]+$/; // Only allows letters and spaces

  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value && !pattern.test(value)) {
      return { 'textOnly': true };
    }

    return null;
  };
}


export function nonZeroValidator(): ValidatorFn {
  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = control.value;

    if (value === 0) {
      return { 'nonZero': true };
    }

    return null;
  };
}

export function notOnlyWhitespaceandZero(): ValidatorFn {
  return (control: AbstractControl): { [key: string]: any } | null => {
    const value = (control.value || '').toString().trim();

    // Remove all spaces
    const noSpaces = value.replace(/\s/g, '');

    // Check: if value is empty OR contains only 0s after removing spaces
    if (noSpaces === '' || /^0+$/.test(noSpaces)) {
      return { 'noWhitespaceAndZero': true }; // Validation failed
    }

    return null; // Validation passed
  };
}

export function onlySpecialCharsValidator(): ValidatorFn {
  // Matches a string that contains ONLY non-alphanumeric characters
  const onlySpecialCharRegex = /^[^a-zA-Z0-9]+$/;

  return (control: AbstractControl): ValidationErrors | null => {
    const value = control.value;

    if (value && onlySpecialCharRegex.test(value)) {
      return { onlySpecialChars: true }; // ❌ invalid if only special chars
    }

    return null; // ✅ valid if contains at least 1 letter/number
  };
}
