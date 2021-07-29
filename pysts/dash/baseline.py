def baseline_als(y, lam=100, p=0.001, n_iter=10, max_mse=0,L=None):
    """
    Estimate baseline of spectrum using the Assymetric Least Squares [1] algorithm.

    Args:
        y:          Input spectrum.
        lam:        (Optional) Smoothing parameter.
        p:          (Optional) Assymetry parameter.
        n_iter:     (Optional) Maximum number of fitting iterations.
        max_mse:    (Optional) Maximum MSE change between fitting iterations that
                    will terminate the fitting process.

    Returns:
        z:          Estimated baseline.

    References:
        [1]     Sung-June Baek, et. al. Baseline correction using asymmetrically
                reweighted penalized least squares smoothing (Analyst, 2015, 140, 250)
    """

    import numpy as np
    from scipy import sparse
    from scipy.sparse.linalg import spsolve

    L = len(y)
    D = sparse.csc_matrix(np.diff(np.eye(L), 2))
    w = np.ones(L)
    for i in range(0, n_iter):
        W = sparse.spdiags(w, 0, L, L)
        Z = W + lam * D.dot(D.transpose())
        if i > 0:
            z_prev = z
        z = spsolve(Z, w * y)
        if i > 0:
            mse = np.sqrt(np.sum(z - z_prev) ** 2) / len(z)
            if mse < max_mse:
                break
        w = p * (y > z) + (1 - p) * (y < z)

    return y-z,z
