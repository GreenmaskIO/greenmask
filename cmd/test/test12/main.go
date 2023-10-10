package main

import "fmt"

// add принимает на вход сигнальный канал для прекращения работы и канал с входными данными для работы,
// а возвращает канал, в который будет отправляться результат вычислений.
// На фоне будет запущена горутина, выполняющая вычисления до момента закрытия doneCh.
func add(doneCh chan struct{}, inputCh chan int) chan int {
	// канал с результатом
	addRes := make(chan int)

	// горутина, в которой добавляем к значению из inputCh единицу и отправляем результат в addRes
	go func() {
		// закрываем канал, когда горутина завершается
		defer close(addRes)

		// берём из канала inputCh значения, которые надо изменить
		for data := range inputCh {
			result := data + 1

			select {
			// если канал doneCh закрылся, выходим из горутины
			case <-doneCh:
				return
			// если doneCh не закрыт, отправляем результат вычисления в канал результата
			case addRes <- result:
			}
		}
	}()
	// возвращаем канал для результатов вычислений
	return addRes
}

// multiply принимает на вход сигнальный канал для прекращения работы и канал с входными данными для работы,
// а возвращает канал, в который будет отправляться результат вычислений.
// На фоне будет запущена горутина, выполняющая вычисления до момента закрытия doneCh.
func multiply(doneCh chan struct{}, inputCh chan int) chan int {
	// канал с результатом
	multiplyRes := make(chan int)

	// горутина, в которой значение из inputCh умножаем на 2 и возвращаем в канал multiplyRes
	go func() {
		// закрываем канал multipleRes, когда горутина завершается
		defer close(multiplyRes)

		// берем из канала inputCh значения, которые надо изменить
		for data := range inputCh {
			// изменяем данные
			result := data * 2

			select {
			// если канал doneCh закрылся, выходим из горутины
			case <-doneCh:
				return
			// если doneCh не закрыт, отправляем результат вычисления в канал результата
			case multiplyRes <- result:
			}
		}
	}()

	// возвращаем канал для результатов вычислений
	return multiplyRes
}

// generator возвращает канал с данными
func generator(doneCh chan struct{}, input []int) chan int {
	// канал, в который будем отправлять данные из слайса
	inputCh := make(chan int)

	// горутина, в которой отправляем в канал  inputCh данные
	go func() {
		// как отправители закрываем канал, когда всё отправим
		defer close(inputCh)

		// перебираем все данные в слайсе
		for _, data := range input {
			select {
			// если doneCh закрыт, сразу выходим из горутины
			case <-doneCh:
				return
			// если doneCh не закрыт, кидаем в канал inputCh данные data
			case inputCh <- data:
			}
		}
	}()

	// возвращаем канал для данных
	return inputCh
}

func main() {
	// ваши данные в слайсе
	input := []int{1, 2, 3, 4, 5, 6, 7, 8}

	// канал для сигнала к выходу из горутины
	doneCh := make(chan struct{})
	// при завершении программы закрываем канал doneCh, чтобы все горутины тоже завершились
	defer close(doneCh)

	// получаем канал с данными с помощью генератора
	inputCh := generator(doneCh, input)

	// ваш конвейер, сначала работает add,  потом multiply
	resultCh := multiply(doneCh, add(doneCh, inputCh))

	// выводим результат
	for res := range resultCh {
		fmt.Println(res)
	}
}
