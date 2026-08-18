import { environment } from "../../../../environments/environment";

export const URLS = Object({
  auth: `${environment.apiUrl}pipeline/auth`,

  fileUpload: `${environment.apiUrl}ocr/ocr/markdown-only`,
  getFile: `${environment.apiUrl}pipeline/files`,

  docType: `${environment.apiUrl}pipeline/document-types`,
  docLogic: `${environment.apiUrl}pipeline/document-logics`,
  schema: `${environment.apiUrl}pipeline/document-schemas`,

  extract: `${environment.apiUrl}ocr/ocr/extract/markdown`,
  webhook: `${environment.apiUrl}pipeline/user-webhooks`,

  alertMail: `${environment.apiUrl}pipeline/alert-mail`,

  apiKeys: `${environment.apiUrl}pipeline/user-apikeys`,

  userSftp: `${environment.apiUrl}pipeline/user-sftp`,
  userSmtp: `${environment.apiUrl}pipeline/user-smtp`,
})
