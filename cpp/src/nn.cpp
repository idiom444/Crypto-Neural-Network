#include "nn.h"

int main() {

  torch::Tensor tensor = torch::rand({2, 3});
  std::cout << tensor << std::endl;



  return EXIT_SUCCESS;
}
