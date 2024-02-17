package validate_utils

//type Documenter interface {
//	Print(w io.Writer, d Documenter) error
//	Complete(w io.Writer) error
//}
//
//type JsonPrinter struct {
//	firstLinePrinted bool
//}
//
//func NewJsonPrinter() *JsonPrinter {
//	return &JsonPrinter{}
//}
//
//func (jp *JsonPrinter) Print(w io.Writer, d Documenter) error {
//	if !jp.firstLinePrinted {
//		_, err := w.Write([]byte("["))
//		if err != nil {
//			return fmt.Errorf("error printing intial line: %w", err)
//		}
//	}
//	_, err := w.Write([]byte(","))
//	if err != nil {
//		return fmt.Errorf("error printing list separator: %w", err)
//	}
//	data, err := d.Data()
//	if err != nil {
//		return fmt.Errorf("error getting document data: %w", err)
//	}
//	_, err = w.Write(data)
//	if err != nil {
//		return fmt.Errorf("error printing document data: %w", err)
//	}
//	return nil
//}
//
//func (jp *JsonPrinter) Complete(w io.Writer) error {
//	// Print the list closing bracket "]"
//	seq := []byte("]")
//	if !jp.firstLinePrinted {
//		// If there was not any document that we print empty document
//		seq = []byte("[]")
//	}
//	if _, err := w.Write(seq); err != nil {
//		return err
//	}
//	return nil
//}
