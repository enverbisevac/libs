package validator

type Validator[T any] func(T) error

func Validate[T any](data T, validators ...Validator[T]) error {
	for _, validator := range validators {
		err := validator(data)
		if err != nil {
			return err
		}
	}
	return nil
}
