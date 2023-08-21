//
// Created by gnandrade on 17/03/2020.
//

#ifndef PQNNS_WS_MATRIX_H
#define PQNNS_WS_MATRIX_H

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

using namespace std;

template <typename T>
class QSMatrix {
private:
    std::vector<std::vector<T>> mat;
    unsigned rows;
    unsigned cols;

public:
    QSMatrix(unsigned _rows, unsigned _cols, const T& _initial);
    QSMatrix(unsigned _rows, unsigned _cols);
    QSMatrix(const QSMatrix<T>& rhs);
    virtual ~QSMatrix();

    // Fill matrix
    void fillRandomly();
    void fillFromFile(string file);

    // Print Matrix
    void head(unsigned count);

    // Operator overloading, for "standard" mathematical matrix operations
    QSMatrix<T>& operator=(const QSMatrix<T>& rhs);

    // Matrix mathematical operations
    QSMatrix<T> operator+(const QSMatrix<T>& rhs);
    QSMatrix<T>& operator+=(const QSMatrix<T>& rhs);
    QSMatrix<T> operator-(const QSMatrix<T>& rhs);
    QSMatrix<T>& operator-=(const QSMatrix<T>& rhs);
    QSMatrix<T> operator*(const QSMatrix<T>& rhs);
    QSMatrix<T>& operator*=(const QSMatrix<T>& rhs);
    QSMatrix<T> transpose();

    // Matrix/scalar operations
    QSMatrix<T> operator+(const T& rhs);
    QSMatrix<T> operator-(const T& rhs);
    QSMatrix<T> operator*(const T& rhs);
    QSMatrix<T> operator/(const T& rhs);

    // Matrix/vector operations
    std::vector<T> operator*(const std::vector<T>& rhs);
    std::vector<T> diag_vec();

    // Access the individual elements
    T& operator()(const unsigned& row, const unsigned& col);
    const T& operator()(const unsigned& row, const unsigned& col) const;

    // Access the row and column sizes
    unsigned get_rows() const;
    unsigned get_cols() const;
    unsigned size() const;
    unsigned dimension() const;
    std::vector<T> getRow(int index);
};

#include "../../src/utils/Matrix.cpp"

#endif // PQNNS_WS_MATRIX_H
